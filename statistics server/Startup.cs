using System;
using System.IO;
using System.Net;
using System.Net.Mime;
using System.Text;
using System.Threading;
using Hangfire;
using Hangfire.Console;
using Hangfire.Dashboard;
using Hangfire.Heartbeat;
using Hangfire.Heartbeat.Server;
using Hangfire.Mongo;
using Hangfire.Mongo.Migration.Strategies;
using Hangfire.Mongo.Migration.Strategies.Backup;
using Hangfire.Storage;
using HangfireBasicAuthenticationFilter;
using Kartrider.Api.AspNetCore;
using Kartrider.Metadata;
using Kartrider.Metadata.AspNetCore;
using Microsoft.AspNetCore.Authentication.JwtBearer;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Microsoft.IdentityModel.Tokens;
using MongoDB.Driver;
using statistics_server.Job;
using statistics_server.Service;

namespace statistics_server;

public class Startup
{
    private readonly TimeSpan _heartBeatInterval = TimeSpan.FromMinutes(1);
    public Startup(IConfiguration configuration, IWebHostEnvironment env)
    {
        Configuration = configuration;
        Env = env;
    }

    private IConfiguration Configuration { get; set; }
    private IWebHostEnvironment Env { get; }

    private void OnApplicationStarted()
    {
        // Job Setup
        RecurringJob.AddOrUpdate<CollectMatchData>("Match_Collect", job => job.Execute(null), "*/30 * * * * *",
            TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Match>("Match_Live",job => job.Live(null), "*/1 * * * * *", TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Match>("Match_Daily",job => job.Daily(null), Cron.Daily, TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Kartbody>("Kartbody_ItemLive",job => job.ItemLive(null), "*/1 * * * * *", TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Kartbody>("Kartbody_ItemDaily",job => job.ItemDaily(null), Cron.Daily, TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Kartbody>("Kartbody_SpeedLive",job => job.SpeedLive(null), "*/1 * * * * *", TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Kartbody>("Kartbody_SpeedDaily",job => job.SpeedDaily(null), Cron.Daily, TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Track>("Track_ItemLive",job => job.ItemLive(null), "*/1 * * * * *", TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Track>("Track_ItemDaily",job => job.ItemDaily(null), Cron.Daily, TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Track>("Track_SpeedLive",job => job.SpeedLive(null), "*/1 * * * * *", TimeZoneInfo.Local);
        RecurringJob.AddOrUpdate<Track>("Track_SpeedDaily",job => job.SpeedDaily(null), Cron.Daily, TimeZoneInfo.Local);

    }

    public void ConfigureServices(IServiceCollection services)
    {
        services.AddControllers();
        var mongoUrlBuilder = new MongoUrlBuilder($"mongodb://{Configuration["mongo:url"]}/hangfire");
        var mongoClient = new MongoClient(mongoUrlBuilder.ToMongoUrl());
        services.AddHangfire(configuration => configuration
            .UseHeartbeatPage(_heartBeatInterval)
            .SetDataCompatibilityLevel(CompatibilityLevel.Version_170)
            .UseSimpleAssemblyNameTypeSerializer()
            .UseRecommendedSerializerSettings()
            .UseConsole()
            .UseMongoStorage(mongoClient, mongoUrlBuilder.DatabaseName, new MongoStorageOptions
            {
                MigrationOptions = new MongoMigrationOptions
                {
                    MigrationStrategy = new MigrateMongoMigrationStrategy(),
                    BackupStrategy = new CollectionMongoBackupStrategy()
                },
                Prefix = "hangfire.mongo",
                CheckConnection = true,
                CheckQueuedJobsStrategy = CheckQueuedJobsStrategy.TailNotificationsCollection
            }));


        // Add the processing server as IHostedService
        services.AddHangfireServer(option =>
        {
            option.ServerName = Environment.MachineName.ToLower();
            option.WorkerCount = Environment.ProcessorCount;
            option.SchedulePollingInterval = TimeSpan.FromSeconds(5);
        });

        services.AddKartriderApi(Configuration["KartriderApiKey"]);

        services.AddKartriderMetadata(option =>
        {
            option.Path = Path.Combine(Path.GetTempPath(), "metadata");
            option.UpdateInterval = 3600;
            option.UpdateNow = true;
        });
        services.AddSingleton<MatchRawDataService>();
        
        services.AddAuthentication(x =>
            {
                x.DefaultAuthenticateScheme = JwtBearerDefaults.AuthenticationScheme;
                x.DefaultChallengeScheme = JwtBearerDefaults.AuthenticationScheme;
            })
            .AddJwtBearer(x =>
            {
                var jwtSecret = Encoding.ASCII.GetBytes(Configuration.GetSection("JwtOptions:Secret").Value);
                x.RequireHttpsMetadata = Environment.GetEnvironmentVariable("ASPNETCORE_ENVIRONMENT")?.Equals("Production") ?? false;
                /*
                 * 토큰을 저장함, 이러면 컨트롤러에서 아래와 같이 토큰에 접근 가능하다.
                 * var accessToken = await HttpContext.GetTokenAsync("access_token");
                 */
                x.SaveToken = false;
                x.TokenValidationParameters = new TokenValidationParameters
                {
                    /*
                     * 유효성 검사
                     * https://docs.microsoft.com/ko-kr/azure/active-directory/develop/scenario-protected-web-api-app-configuration
                     */

                    ValidateIssuerSigningKey = true,
                    IssuerSigningKey = new SymmetricSecurityKey(jwtSecret),
                    ValidateIssuer = false,
                    ValidateAudience = false,
                    ClockSkew = TimeSpan.Zero
                };
            });
    }

    // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
    public void Configure(IApplicationBuilder app, IWebHostEnvironment env,IKartriderMetadata kartriderMetadata)
    {      // processing 이었던 Job 모두 삭제
        foreach (var job in JobStorage.Current.GetMonitoringApi().ProcessingJobs(0,int.MaxValue))
        {
            BackgroundJob.Delete(job.Key);
        }  
        //등록된 모든 RecurringJob 삭제
        foreach (var job in JobStorage.Current.GetConnection().GetRecurringJobs())
        {
            RecurringJob.RemoveIfExists(job.Id);
        }
        bool init = false;
        kartriderMetadata.OnUpdated += (metadata, run) =>
        {
            if (init)
            {
                return;
            }

            init = true;
            var webClient = new WebClient();
            UpdateFromUrl("https://raw.githubusercontent.com/mschadev/kartrider-open-api-docs/master/metadata/track.json", MetadataType.Track);
            UpdateFromUrl("https://raw.githubusercontent.com/mschadev/kartrider-open-api-docs/master/metadata/channel.json", ExtendMetadataType.Channel);
            OnApplicationStarted();
            void UpdateFromUrl<T>(string url, T metadataType) where T : Enum
            {
                string path = Path.Combine(Path.GetTempPath(), Path.GetTempFileName());
                webClient.DownloadFile(url, path);
                metadata.MetadataUpdate(path, metadataType);
                File.Delete(path);
            }
        };
        if (env.IsDevelopment()) app.UseDeveloperExceptionPage();
        app.UseHangfireDashboard("/hangfire", new DashboardOptions
        {
            Authorization = new IDashboardAuthorizationFilter[]
            {
                new HangfireCustomBasicAuthenticationFilter
                    {User = Configuration["Hangfire:Id"], Pass = Configuration["Hangfire:Pw"]}
            },
        });
        app.UseRouting();
        app.UseAuthentication(); 
        app.UseAuthorization();
        app.UseEndpoints(endpoints =>
        {
            // root 
            // https://docs.hangfire.io/en/latest/configuration/using-dashboard.html#change-url-mapping
            // endpoints.MapHangfireDashboard("/hangfire");
            endpoints.MapControllers();
        });
        app.UseHangfireServer(additionalProcesses: new[] {new ProcessMonitor(checkInterval: _heartBeatInterval)});
    }
}