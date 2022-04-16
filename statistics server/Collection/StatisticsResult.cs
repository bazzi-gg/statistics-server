using MongoDB.Bson.Serialization.Attributes;
using System;
namespace statistics_server.Collection;

public class StatisticsResult<T>
{
    [BsonId]
    public string Id { get; set; }
    public T Data { get; set; }
    public DateTime LastUpdated { get; set; }
}