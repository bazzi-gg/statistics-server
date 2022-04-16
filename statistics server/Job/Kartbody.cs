using System.Collections.Generic;
using System.Runtime.InteropServices;
using Hangfire.Server;
using Kartrider.Metadata;
using statistics_server.Collection;
using statistics_server.Service;
using System;
using System.Linq;
using System.Text.Json;
using Hangfire.Console;
using Microsoft.Extensions.Configuration;
using MongoDB.Driver;
using statistics_server.Hangfire.Attribute;
using statistics_server.Model;

namespace statistics_server.Job;

public class Kartbody
{
    private readonly MatchRawDataService _matchDataService;
    private readonly IKartriderMetadata _kartriderMetadata;
    private readonly IConfiguration _configuration;
    public Kartbody(MatchRawDataService matchRawDataService,IKartriderMetadata kartriderMetadata,IConfiguration configuration)
    {
        _matchDataService = matchRawDataService;
        _kartriderMetadata = kartriderMetadata;
        _configuration = configuration;
    }

    [SkipWhenPreviousJobIsRunning]
    public void ItemLive(PerformContext context)
    {
        Execute(DateTime.Now, context,"kartbody.item",new [] { "itemIndiCombine", "itemTeamCombine" });
    }
    [SkipWhenPreviousJobIsRunning]
    public void ItemDaily(PerformContext context)
    {
        var dateTime = DateTime.Now.AddDays(-1).Date.AddHours(23).AddMinutes(59).AddSeconds(59).AddMilliseconds(999);
        Execute(dateTime, context,"kartbody.item",new [] { "itemIndiCombine", "itemTeamCombine" });
    }
    [SkipWhenPreviousJobIsRunning]
    public void SpeedLive(PerformContext context)
    {
        Execute(DateTime.Now, context,"kartbody.speed",new [] {"speedIndiCombine", "speedTeamCombine" });
    }
    [SkipWhenPreviousJobIsRunning]
    public void SpeedDaily(PerformContext context)
    {
        var dateTime = DateTime.Now.AddDays(-1).Date.AddHours(23).AddMinutes(59).AddSeconds(59).AddMilliseconds(999);
        Execute(dateTime, context,"kartbody.speed",new [] { "speedIndiCombine", "speedTeamCombine" });
    }

    private void Execute(DateTime dateTime, PerformContext context,string collectionName, string[] channels)
    {
        var matchDatas = _matchDataService.Gets(dateTime).Where(p => channels.Any(x => x == p.Data.Channel)).ToList();
        if (matchDatas.Count == 0)
        {
            return;
        }
        
        var client = new MongoClient($"mongodb://{_configuration["mongo:url"]}");
        var collection = client.GetDatabase("app").GetCollection<StatisticsResult<List<KartbodyData>>>(collectionName);
        var documentId = dateTime.ToString("yyyy-MM-dd");
        var document = collection.Find(x => x.Id == documentId).FirstOrDefault() ?? new StatisticsResult<List<KartbodyData>>() {Data = new List<KartbodyData>()};
        
        context.WriteLine($"match count: ${matchDatas.Count}");
        var kartbodyOrderByDescendingPickRate = matchDatas
            .SelectMany(p => p.Data.Players)
            //.Where(p => !string.IsNullOrEmpty(p.Kartbody))
            .GroupBy(p => p.Kartbody)
            .OrderByDescending(p => p.Count())
            .Select(p => p.Key)
            //.Take(6)
            .ToList();
        context.WriteLine($"detected kartbody count: {kartbodyOrderByDescendingPickRate.Count}");
        var result = new Dictionary<string, KartbodyData>();
        var playedPlayerCount = matchDatas.Sum(x => x.Data.Players.Count);
        foreach (string kartbodyHash in kartbodyOrderByDescendingPickRate)
        {
            int pickedMatchCount = matchDatas
                .Count(p => p.Data.Players.Any(x => x.Kartbody == kartbodyHash));
            double pickRate = (double)pickedMatchCount / playedPlayerCount * 100;
            result.Add(kartbodyHash, new KartbodyData()
            {
                Name = _kartriderMetadata[MetadataType.Kart, kartbodyHash, "알 수 없음"],
                PickRate = pickRate,
                NameHash = kartbodyHash,
                Count = pickedMatchCount
            });
        }

        document.Data = result.Select(x => x.Value).OrderByDescending(x => x.PickRate).ToList();
        document.LastUpdated = DateTime.Now;
        if (string.IsNullOrEmpty(document.Id))
        {
            document.Id = documentId;
            collection.InsertOne(document);
        }
        else
        {
            collection.ReplaceOne(x => x.Id == documentId, document);
        }
        context.WriteLine("success");
    }
}