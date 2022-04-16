using System;
using System.Collections.Generic;
using System.Linq;
using Hangfire.Console;
using Hangfire.Server;
using Kartrider.Metadata;
using Microsoft.Extensions.Configuration;
using MongoDB.Driver;
using statistics_server.Collection;
using statistics_server.Hangfire.Attribute;
using statistics_server.Model;
using statistics_server.Service;

namespace statistics_server.Job;

public class Track
{
      private readonly MatchRawDataService _matchDataService;
    private readonly IKartriderMetadata _kartriderMetadata;
    private readonly IConfiguration _configuration;
    public Track(MatchRawDataService matchRawDataService,IKartriderMetadata kartriderMetadata,IConfiguration configuration)
    {
        _matchDataService = matchRawDataService;
        _kartriderMetadata = kartriderMetadata;
        _configuration = configuration;
    }

    [SkipWhenPreviousJobIsRunning]
    public void ItemLive(PerformContext context)
    {
        Execute(DateTime.Now, context,"track.item",new [] { "itemIndiCombine", "itemTeamCombine" });
    }
    [SkipWhenPreviousJobIsRunning]
    public void ItemDaily(PerformContext context)
    {
        var dateTime = DateTime.Now.AddDays(-1).Date.AddHours(23).AddMinutes(59).AddSeconds(59).AddMilliseconds(999);
        Execute(dateTime, context,"track.item",new [] { "itemIndiCombine", "itemTeamCombine" });
    }
    [SkipWhenPreviousJobIsRunning]
    public void SpeedLive(PerformContext context)
    {
        Execute(DateTime.Now, context,"track.speed",new [] {"speedIndiCombine", "speedTeamCombine" });
    }
    [SkipWhenPreviousJobIsRunning]
    public void SpeedDaily(PerformContext context)
    {
        var dateTime = DateTime.Now.AddDays(-1).Date.AddHours(23).AddMinutes(59).AddSeconds(59).AddMilliseconds(999);
        Execute(dateTime, context,"track.speed",new [] { "speedIndiCombine", "speedTeamCombine" });
    }

    private void Execute(DateTime dateTime, PerformContext context,string collectionName, string[] channels)
    {
        var matchDatas = _matchDataService.Gets(dateTime).Where(p => channels.Any(x => x == p.Data.Channel)).ToList();
        if (matchDatas.Count == 0)
        {
            return;
        }
        
        var client = new MongoClient($"mongodb://{_configuration["mongo:url"]}");
        var collection = client.GetDatabase("app").GetCollection<StatisticsResult<List<TrackData>>>(collectionName);
        var documentId = dateTime.ToString("yyyy-MM-dd");
        var document = collection.Find(x => x.Id == documentId).FirstOrDefault() ??
                       new StatisticsResult<List<TrackData>>() {Data = new List<TrackData>()};
        
        int count = matchDatas.Count;
        context.WriteLine($"match count: ${count}");
        var tracks = matchDatas.Select(x => x.Data.TrackId)
            .ToHashSet();
        context.WriteLine($"detected track count: {tracks.Count}");
        var result = new Dictionary<string, TrackData>();
        foreach (string trackHash in tracks)
        {
            int pickedMatchCount = matchDatas.Count(p => p.Data.TrackId == trackHash);
            double pickRate = (double)pickedMatchCount / count * 100;
            result.Add(trackHash, new TrackData()
            {
                Name = _kartriderMetadata[MetadataType.Track,trackHash, "알 수 없음"],
                PickRate = pickRate,
                NameHash = trackHash,
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