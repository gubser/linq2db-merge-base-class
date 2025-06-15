using LinqToDB;
using LinqToDB.Data;
using LinqToDB.DataProvider.PostgreSQL;
using LinqToDB.Mapping;
using Microsoft.Extensions.Logging;
using Npgsql;
using Testcontainers.PostgreSql;

namespace Linq2DbMergeBaseClass;


public class MyBaseClass
{
    [Column("id"), PrimaryKey]
    public int Id { get; set; }
}

[Table("mytable")]
public class MyChildClass : MyBaseClass
{
    [Column("value")]
    public int Value { get; set; }
}

public class Tests
{

    ILoggerFactory _loggerFactory;
    PostgreSqlContainer _pg;
    NpgsqlDataSource _dataSource;
    DataOptions _dataOptions;

    private static NpgsqlDataSource CreateDataSource(string connectionString, ILoggerFactory loggerFactory)
    {
        var builder = new NpgsqlDataSourceBuilder(connectionString);

        builder.EnableParameterLogging();
        builder.UseLoggerFactory(loggerFactory);

        return builder.Build();
    }

    private static DataOptions CreateDataOptions(string connectionString, NpgsqlDataSource dataSource)
    {
        var dataProvider = PostgreSQLTools.GetDataProvider(connectionString: connectionString);

        return new DataOptions()
            .UseConnectionFactory(dataProvider, _ => dataSource.CreateConnection());
    }


    [OneTimeSetUp]
    public async Task OneTimeSetUp()
    {
        _loggerFactory = LoggerFactory.Create(builder =>
            builder.AddProvider(new TestContextLoggerProvider()).SetMinimumLevel(LogLevel.Debug));

        _pg = new PostgreSqlBuilder().WithImage("postgres:17-alpine").Build();
        await _pg.StartAsync();
        _dataSource = CreateDataSource(_pg.GetConnectionString(), _loggerFactory);
        _dataOptions = CreateDataOptions(_pg.GetConnectionString(), _dataSource);

        using var conn = new DataConnection(_dataOptions);
        conn.CreateTable<MyChildClass>();

    }

    [OneTimeTearDown]
    public async Task OneTimeTearDown()
    {
        await _dataSource.DisposeAsync();
        await _pg.DisposeAsync();
        _loggerFactory.Dispose();
    }

    [Test]
    public void Test2()
    {
        using var conn = new DataConnection(_dataOptions);

        List<MyChildClass> items = [
            new MyChildClass { Value = 1 },
            new MyChildClass { Value = 2 },
            new MyChildClass { Value = 3 },
            new MyChildClass { Value = 4 },
        ];

        int affected =
            conn.GetTable<MyChildClass>()
                .Merge()
                .Using(items)
                .OnTargetKey()
                .InsertWhenNotMatched()
                .Merge();

        Assert.That(affected, Is.EqualTo(4));
    }
}
