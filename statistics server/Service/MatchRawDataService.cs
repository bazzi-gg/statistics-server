using System;
using System.Collections.Generic;
using System.Linq;
using Microsoft.Extensions.Configuration;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using statistics_server.Model;
using MongoDB.Driver.Linq;
using statistics_server.Collection;

namespace statistics_server.Service;

public class MatchRawDataService
{
    private readonly  IMongoCollection<MatchRawData> _collection;

    public MatchRawDataService(IConfiguration configuration)
    {
        var client = new MongoClient($"mongodb://{configuration["mongo:url"]}");

        _collection = client.GetDatabase("app").GetCollection<MatchRawData>("match.datas");
        var indexOptions = new CreateIndexOptions
        {
            ExpireAfter = TimeSpan.FromDays(3),
            Name = "CreatedAt"
        };
        // Collection에 Index가 있는지 체크
        var existExpireAtIndex = _collection.Indexes.List()
            .ToList()
            .SelectMany(x => x.Elements)
            .Select(x => x.Value.ToString())
            .Contains(indexOptions.Name);
        
        if (existExpireAtIndex)
        {
            // Index가 있으면 Drop
            _collection.Indexes.DropOne(indexOptions.Name);
        }
        
        var indexKeys = Builders<MatchRawData>.IndexKeys.Ascending(data => data.CreatedAt);
        var indexModel = new CreateIndexModel<MatchRawData>(indexKeys, indexOptions);
        _collection.Indexes.CreateOne(indexModel);
    }
    

    public List<MatchRawData> Gets(DateTime dateTime)
    {
        // DB에 저장되면 UTC로 저장되므로 가져올 땐 다시 +9해서 찾는다.
        var startDateTime = dateTime.Date;
        return _collection.Find(x => x.Data.StartDateTime >= startDateTime && x.Data.StartDateTime <= dateTime).ToList();
    }
    
    public MatchRawData Get(string matchId)
    {
        return _collection.Find(p => p.Id == matchId).FirstOrDefault();
    }

    public MatchRawData Create(MatchRawData matchData)
    {
        _collection.InsertOne(matchData);
        return matchData;
    }
}