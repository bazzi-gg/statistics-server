using System;
using Kartrider.Api.Endpoints.MatchEndpoint.Models;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;

namespace statistics_server.Collection;

public class MatchRawData
{
    [BsonId]
    public string Id { get; set; }

    public MatchDetail Data { get; set; }
    [BsonDateTimeOptions(Kind = DateTimeKind.Local)]
    public DateTime CreatedAt { get; set; }= DateTime.Now;

    public MatchRawData(MatchDetail matchDetail)
    {
        Id = matchDetail.MatchId;
        Data = matchDetail;
    }
}