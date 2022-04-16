using Kartrider.Api.Endpoints.MatchEndpoint.Models;

namespace statistics_server.Model;

public class PlayerPlayData
{
    public string Nickname { get; set; }
    public string CharacterHash { get; set; }
    public string Character { get; set; }
    public int Count { get; set; }
}