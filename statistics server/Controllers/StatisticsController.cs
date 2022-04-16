using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Configuration;
using MongoDB.Driver;
using statistics_server.Collection;
using statistics_server.Model;

namespace statistics_server.Controllers;
[Authorize]
[Route("[controller]")]
[ApiController]
public class StatisticsController : ControllerBase
{
    private readonly IConfiguration _configuration;
    private readonly IMongoDatabase _database;
    public StatisticsController(IConfiguration configuration)
    {
        _configuration = configuration;
        var client = new MongoClient($"mongodb://{_configuration["mongo:url"]}");
        _database = client.GetDatabase("app");
    }
    
    [HttpGet("item-kartbody")]
    // GET
    public async Task<ActionResult<StatisticsResult<IEnumerable<KartbodyData>>>> GetItemKartbody(string date, int limit = 5)
    {
        var collection = _database.GetCollection<StatisticsResult<IEnumerable<KartbodyData>>>("kartbody.item");
        var data = (await collection.FindAsync(x => x.Id == date)).SingleOrDefault();
        if (data == null)
        {
            return NoContent();
        }

        data.Data = data.Data.Take(limit);
        return Ok(data);
    }
    [HttpGet("speed-kartbody")]
    // GET
    public async Task<ActionResult<StatisticsResult<IEnumerable<KartbodyData>>>> GetSpeedKartbody(string date, int limit = 5)
    {
        var collection = _database.GetCollection<StatisticsResult<IEnumerable<KartbodyData>>>("kartbody.speed");
        var data = (await collection.FindAsync(x => x.Id == date)).SingleOrDefault();
        if (data == null)
        {
            return NoContent();
        }

        data.Data = data.Data.Take(limit);
        return Ok(data);
    }
    [HttpGet("speed-track")]
    // GET
    public async Task<ActionResult<StatisticsResult<IEnumerable<TrackData>>>> GetItemTrack(string date, int limit = 5)
    {
        var collection = _database.GetCollection<StatisticsResult<IEnumerable<TrackData>>>("track.speed");
        var data = (await collection.FindAsync(x => x.Id == date)).SingleOrDefault();
        if (data == null)
        {
            return NoContent();
        }

        data.Data = data.Data.Take(limit);
        return Ok(data);
    }
    [HttpGet("item-track")]
    // GET
    public async Task<ActionResult<StatisticsResult<IEnumerable<TrackData>>>> GetSpeedTrack(string date, int limit = 5)
    {
        var collection = _database.GetCollection<StatisticsResult<IEnumerable<TrackData>>>("track.item");
        var data = (await collection.FindAsync(x => x.Id == date)).SingleOrDefault();
        if (data == null)
        {
            return NoContent();
        }

        data.Data = data.Data.Take(limit);
        return Ok(data);
    }
    [HttpGet("match")]
    // GET
    public async Task<ActionResult<StatisticsResult<MatchData>>> GetMatch(string date)
    {
        var collection = _database.GetCollection<StatisticsResult<MatchData>>("match.result");
        var data = (await collection.FindAsync(x => x.Id == date)).SingleOrDefault();
        if (data == null)
        {
            return NoContent();
        }
        return Ok(data);
    }
}