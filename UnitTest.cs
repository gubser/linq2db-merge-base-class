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

    [Column("value_a")]
    public int ValueA { get; set;  }
}

[Table("mytable")]
public class MyChildClass : MyBaseClass
{
    [Column("value_b")]
    public int ValueB { get; set; }
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
            new MyChildClass { ValueA = 1, ValueB = 1 },
            new MyChildClass { ValueA = 2, ValueB = 2 },
            new MyChildClass { ValueA = 3, ValueB = 3 },
            new MyChildClass { ValueA = 4, ValueB = 4 },
        ];

        int affected =
            conn.GetTable<MyChildClass>()
                .Merge()
                .Using(items)
                .On((x, y) => x.ValueA == y.ValueA && x.ValueB == y.ValueB)
                .InsertWhenNotMatched()
                .Merge();

        Assert.That(affected, Is.EqualTo(4));
    }
}
