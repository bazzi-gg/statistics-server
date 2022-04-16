using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using Hangfire;
using Hangfire.Console;
using Hangfire.Server;
using Kartrider.Api.Endpoints.MatchEndpoint.Models;
using Kartrider.Metadata;
using Microsoft.Extensions.Configuration;
using MongoDB.Driver;
using statistics_server.Collection;
using statistics_server.Hangfire.Attribute;
using statistics_server.Model;
using statistics_server.Service;

namespace statistics_server.Job;

public class Match
{
    private readonly MatchRawDataService _matchDataService;
    private readonly IKartriderMetadata _kartriderMetadata;
    private readonly IConfiguration _configuration;
    public Match(MatchRawDataService matchRawDataService,IKartriderMetadata kartriderMetadata,IConfiguration configuration)
    {
        _matchDataService = matchRawDataService;
        _kartriderMetadata = kartriderMetadata;
        _configuration = configuration;
    }
    
    [SkipWhenPreviousJobIsRunning]
    public void Live(PerformContext context)
    {
        Execute(DateTime.Now, context);
    }
    [SkipWhenPreviousJobIsRunning]
    public void Daily(PerformContext context)
    {
        var dateTime = DateTime.Now.AddDays(-1).Date.AddHours(23).AddMinutes(59).AddSeconds(59).AddMilliseconds(999);
        Execute(dateTime,context);
    }
    private void Execute(DateTime dateTime,PerformContext context)
    {
        var matchDatas = _matchDataService.Gets(dateTime);
        if (matchDatas.Count == 0)
        {
            return;
        }
        foreach (var matchData in matchDatas)
        {
            matchData.Data.StartDateTime = matchData.Data.StartDateTime.AddHours(9);
            matchData.Data.EndDateTime = matchData.Data.EndDateTime.AddHours(9);
        }
        var client = new MongoClient($"mongodb://{_configuration["mongo:url"]}");

        var collection = client.GetDatabase("app").GetCollection<StatisticsResult<MatchData>>("match.result");
        var documentId = dateTime.ToString("yyyy-MM-dd");
        var result = collection.Find(x => x.Id == documentId).FirstOrDefault() ?? new StatisticsResult<MatchData>() {Data = new MatchData()};
        context.WriteLine("PlayerCount..");
        result.Data.PlayerCount = PlayerCount(matchDatas);
        context.WriteLine("PlayerCount..OK");
        context.WriteLine("HotTime..");
        result.Data.HotTime = HotTime(matchDatas);
        context.WriteLine("HotTime..OK");
        context.WriteLine("MostPlayedPlayer..");
        result.Data.MostPlayedPlayer = MostPlayedPlayer(matchDatas);
        context.WriteLine("MostPlayedPlayer..OK");
        context.WriteLine("PlayerTimeCountByMatchType..");
        result.Data.PlayTimeCountByChannel = PlayTimeCountByChannel(matchDatas);
        context.WriteLine("PlayerTimeCountByMatchType..OK");
        context.WriteLine("PopularMatchType..");
        result.Data.PopularChannel = PopularChannel(matchDatas);
        context.WriteLine("PopularMatchType..OK");
        context.WriteLine("UnPopularMatchType..");
        result.Data.UnPopularChannel = UnpopularChannel(matchDatas);
        context.WriteLine("UnPopularMatchType..OK");
        context.WriteLine("OK");
        result.Data.MatchCount = matchDatas.Count;
        result.LastUpdated = DateTime.Now;
        if (string.IsNullOrEmpty(result.Id))
        {
            result.Id = documentId;
            collection.InsertOne(result);
        }
        else
        {
            collection.ReplaceOne(x => x.Id == documentId, result);
        }
    }

    /// <summary>
    /// 한판이라도 플레이한 유저 수
    /// </summary>
    /// <param name="matchDatas">매치 데이터</param>
    /// <returns>플레이어 수</returns>
    private int PlayerCount(IEnumerable<MatchRawData> matchDatas)
    {
        return matchDatas.SelectMany(x => x.Data.Players)
            .DistinctBy(x => x.AccessId)
            .Count();
    }

    /// <summary>
    /// 가장 플레이수가 많은 채널
    /// </summary>
    /// <param name="matchDatas">매치 데이터</param>
    /// <returns>매치타입 hash</returns>
    private string PopularChannel(IEnumerable<MatchRawData> matchDatas)
    {
        string channel = matchDatas.GroupBy(x => x.Data.Channel)
            .OrderByDescending(x => x.Count())
            .ElementAt(0).Key;
        return _kartriderMetadata[ExtendMetadataType.Channel.ToString(), channel, channel];
    }

    /// <summary>
    /// 가장 플레이수가 적은 매치타입(모드)
    /// </summary>
    /// <param name="matchDatas">매치 데이터</param>
    /// <returns>매치타입 hash</returns>
    private string UnpopularChannel(IEnumerable<MatchRawData> matchDatas)
    {
        string channel = matchDatas.GroupBy(x => x.Data.Channel)
            .OrderBy(x => x.Count())
            .ElementAt(0).Key;
        return _kartriderMetadata[ExtendMetadataType.Channel.ToString(), channel, channel];
    }
    
    /// <summary>
    /// 가장 많이 플레이한 시간대
    /// </summary>
    /// <param name="matchDatas">매치 데이터</param>
    /// <returns>24시간제로 리턴함</returns>
    private int HotTime(IEnumerable<MatchRawData> matchDatas)
    {
        return matchDatas.GroupBy(x => x.Data.StartDateTime.Hour)
            .OrderBy(x => x.Count())
            .ElementAt(0).Key;
    }

    /// <summary>
    /// 가장 많이 플레이한 유저
    /// </summary>
    /// <param name="matchDatas">매치 데이터</param>
    /// <param name="limit">추출할 데이터 수 제한</param>
    /// <returns>내림차순으로 가장 많이 플레이한 유저</returns>
    private List<PlayerPlayData> MostPlayedPlayer(IEnumerable<MatchRawData> matchDatas, int limit = 5)
    {
        var playerTable = matchDatas
            .SelectMany(x => x.Data.Players)
            .DistinctBy(x => x.AccessId)
            .ToDictionary(x => x.AccessId, x => x);
        var playerWithPlayCount = new Dictionary<string, int>();
        foreach (var player in matchDatas.SelectMany(x=>x.Data.Players))
        {
            if (!playerWithPlayCount.ContainsKey(player.AccessId))
            {
                playerWithPlayCount.Add(player.AccessId,0);
            }
            playerWithPlayCount[player.AccessId]++;
        }

        var playerList = playerWithPlayCount.OrderByDescending(x => x.Value).Take(limit);
        var res = new List<PlayerPlayData>();
        foreach (var keyValuePair in playerList)
        {
            var accessId = keyValuePair.Key;
            var playCount = keyValuePair.Value;
            res.Add(new PlayerPlayData()
            {
                Character = _kartriderMetadata[MetadataType.Character, playerTable[accessId].Character, "알 수 없음"],
                CharacterHash = playerTable[accessId].Character,
                Count = playCount,
                Nickname = playerTable[accessId].Nickname
            });
        }

        return res;
    }

    /// <summary>
    /// 매치타입별 시계열 플레이 카운트
    /// </summary>
    /// <param name="matchDatas"></param>
    /// <returns></returns>
    private Dictionary<string, List<int>> PlayTimeCountByChannel(IEnumerable<MatchRawData> matchDatas)
    {
        var matchesByChannel = matchDatas
            .GroupBy(p => p.Data.Channel);
        Dictionary<string, List<int>> result = new Dictionary<string, List<int>>();
        foreach (var matches in matchesByChannel)
        {
            string key = _kartriderMetadata[ExtendMetadataType.Channel.ToString(), matches.Key];
            if (!result.ContainsKey(key))
            {
                result.Add(key, new List<int>(new int[24]));
            }

            foreach (var match in matches)
            {
                result[key][match.Data.StartDateTime.Hour]++;
            }
        }
        if (result.ContainsKey(""))
        {
            result.Remove("");
        }
        return result;
    }

}