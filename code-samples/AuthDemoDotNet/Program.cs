using System;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Threading.Tasks;
using System.Text;
using System.Text.Json;
using Azure.Identity;
using Azure.Core;
using Npgsql;  // Install via: dotnet add package Npgsql

class Program
{
    static async Task Main()
    {
        // ====== CONFIGURATION ======
        string tenantId = "<tenant-id from azure portal>";
        string clientId = "<client-id from azure portal>";
        string clientSecret = "<client-secret from azure portal>";
        string databricksHost = "<databricks-instance-url>"; // e.g. https://adb-1234567890123456.11.azuredatabricks.net
        string instanceName = "<postgres-instance-name>";
        string databaseName = "databricks_postgres";
        int port = 5432;

        // ====== STEP 1: AUTHENTICATE TO DATABRICKS VIA AZURE ENTRA ID ======
        var credential = new ClientSecretCredential(tenantId, clientId, clientSecret);
        var tokenContext = new TokenRequestContext(new[] { "2ff814a6-3304-4ab8-85cb-cd0e6f879c1d/.default" });
        AccessToken token = await credential.GetTokenAsync(tokenContext);

        using var httpClient = new HttpClient();
        httpClient.DefaultRequestHeaders.Authorization =
            new AuthenticationHeaderValue("Bearer", token.Token);

        // ====== STEP 2: LOOK UP DATABASE INSTANCE ======
        string getInstanceUrl = $"{databricksHost}/api/2.0/database/instances/{instanceName}";
        var instanceResp = await httpClient.GetAsync(getInstanceUrl);
        // instanceResp.EnsureSuccessStatusCode();

        var instanceJson = await instanceResp.Content.ReadAsStringAsync();
        Console.WriteLine("Instance Info:");
        Console.WriteLine(instanceJson);

        // ====== STEP 3: GENERATE DATABASE CREDENTIAL ======
        var requestId = Guid.NewGuid().ToString();
        var payload = new
        {
            request_id = requestId,
            instance_names = new[] { instanceName }
        };

        var generateCredUrl = $"{databricksHost}/api/2.0/database/credentials";
        var content = new StringContent(JsonSerializer.Serialize(payload), Encoding.UTF8, "application/json");

        var credResp = await httpClient.PostAsync(generateCredUrl, content);
        credResp.EnsureSuccessStatusCode();

        var credJson = await credResp.Content.ReadAsStringAsync();
        var cred = JsonDocument.Parse(credJson);
        string password = cred.RootElement.GetProperty("token").GetString()
            ?? throw new InvalidOperationException("Credential token is missing");

        // ====== STEP 4: CONNECT TO POSTGRES (Lakebase) ======
        // extract read_write_dns from instanceJson
        var instanceDoc = JsonDocument.Parse(instanceJson);
        string host = instanceDoc.RootElement.GetProperty("read_write_dns").GetString()
            ?? throw new InvalidOperationException("read_write_dns is missing in instance metadata");

        string user = clientId; // or use current_user.me().user_name equivalent if known

        string connString = $"Host={host};Port={port};Database={databaseName};Username={user};Password={password};SSL Mode=Require";

        using var conn = new NpgsqlConnection(connString);
        await conn.OpenAsync();

        using var cmd = new NpgsqlCommand("SELECT version()", conn);
        var version = await cmd.ExecuteScalarAsync();

        Console.WriteLine($"Connected to PostgreSQL database. Version: {version}");

        // ====== STEP 5: RUN SAMPLE QUERY ======
        const string sampleQuery = "SELECT * FROM databricks_postgres.public.store_sales LIMIT 10";
        using var sampleCmd = new NpgsqlCommand(sampleQuery, conn);
        using var reader = await sampleCmd.ExecuteReaderAsync();

        Console.WriteLine("First 10 rows from databricks_postgres.public.store_sales:");
        int rowNum = 0;
        while (await reader.ReadAsync())
        {
            rowNum++;
            var values = new object[reader.FieldCount];
            reader.GetValues(values);
            Console.WriteLine($"Row {rowNum}: " + string.Join(", ", values));
        }
    }
}
