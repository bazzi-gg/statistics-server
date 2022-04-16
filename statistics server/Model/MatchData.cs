using System.Collections.Generic;

namespace statistics_server.Model;

public class MatchData
{
    public int PlayerCount { get; set; }
    public int MatchCount { get; set; }
    public int HotTime { get; set; }
    public List<PlayerPlayData> MostPlayedPlayer { get; set; }
    public Dictionary<string,List<int>> PlayTimeCountByChannel { get; set; }
    public string PopularChannel { get; set; }
    public string UnPopularChannel { get; set; }
}