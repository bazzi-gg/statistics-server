using System;
using System.Linq;
using System.Text.RegularExpressions;
using System.Threading.Tasks;
using Hangfire;
using Hangfire.Console;
using Hangfire.Server;
using Kartrider.Api;
using Kartrider.Api.Endpoints.MatchEndpoint.Models;
using Kartrider.Metadata;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;
using MongoDB.Driver;
using statistics_server.Collection;
using statistics_server.Model;
using statistics_server.Service;

namespace statistics_server.Job;

public class CollectMatchData
{
    private readonly IKartriderApi _kartriderApi;
    private readonly ILogger<CollectMatchData> _logger;
    private readonly MatchRawDataService _matchDataService;
    public CollectMatchData(ILogger<CollectMatchData> logger,
        IKartriderApi kartriderApi,
        MatchRawDataService matchRawDataService,
        IConfiguration configuration)
    {
        _logger = logger;
        _kartriderApi = kartriderApi;
        _matchDataService = matchRawDataService;
        var client = new MongoClient($"mongodb://{configuration["mongo:url"]}");
    }
    [JobDisplayName("Collect match data")]
    [MaximumConcurrentExecutions(2)]
    [AutomaticRetry(Attempts = 3)]
    public async Task Execute(PerformContext context)
    {
        var allMatches = await _kartriderApi.Match.GetAllMatchesAsync(null, null, 0, 200);
        context.WriteLine("Get all matches: OK");
        var matchIds = allMatches.Matches.SelectMany(p => p.Value).ToList();
        var progress = context.WriteProgressBar();
        foreach (var matchId in matchIds.WithProgress(progress))
        {
            MatchDetail matchDetail = null;
            try
            {
                matchDetail = await _kartriderApi.Match.GetMatchDetailAsync(matchId);
            }
            catch (KartriderApiException e)
            {
                _logger.LogWarning($"Kartrider API Exception: {e.HttpStatusCode}");
                continue;
            }

            if (_matchDataService.Get(matchDetail.MatchId) != null) continue;
            /* MongoDB에 저장될 때, 무조건 UTC로 계산해서 저장되므로, 한국시간으로 맞춰줄려면 18시간을 더해야 한다.
             * matchDetail.StartDateTime, matchDetail.endDateTime은 이미 UTC이다.
             * +9시 한국시간, 여기서 저장되면 다시 UTC로 저장되므로, 추가로 +9시 해준다.
             */
             matchDetail.StartDateTime = matchDetail.StartDateTime.AddHours(9);
             matchDetail.EndDateTime = matchDetail.EndDateTime.AddHours(9);
            _matchDataService.Create(new MatchRawData(matchDetail));
        }
    }
}