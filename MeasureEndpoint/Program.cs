using System;
using System.Linq;
using Npgsql;
using Qlik.Sense.RestClient;
using Qlik.Sense.RestClient.Qrs;

namespace MeasureEndpoint
{
	internal class Program
	{
		const string host = "<host>";
		const string certsPath = @"C:\path\to\certs";
		const string pwd = "<pwd>";
		private const string user = "<userDir>\\<userId>";

		static void Main(string[] args)
		{

			var certs = RestClient.LoadCertificateFromDirectory(certsPath);
			var factory = new ClientFactory("https://" + host, certs);
			var client = factory.GetClient(new User(user));

			MeasureTransactions(client, "/qrs/dataconnection/full?privileges=true", 10);
		}

		private static NpgsqlDataSource _dataSource;
		private static NpgsqlCommand _countQuery;
		private static NpgsqlCommand CountQuery = _countQuery ?? (_countQuery = CreateCountQuery());

		private static NpgsqlCommand CreateCountQuery()
		{
			var connectionString = $"Host={host};Port=4432;Username=postgres;Password={pwd};Database=QSR";
			_dataSource = NpgsqlDataSource.Create(connectionString);
			return _dataSource.CreateCommand("SELECT SUM(xact_commit + xact_rollback) FROM pg_stat_database");
		}

		private static void MeasureTransactions(IRestClient client, string endpoint, int repetitions = 1)
		{
			if (repetitions == 1)
				MeasureTransactionsSingle(client, endpoint);
			else
			{
				var minCnt = int.MaxValue;
				foreach (var i in Enumerable.Range(1, repetitions))
				{
					Console.Write($"{i}/{repetitions}\t");
					minCnt = Math.Min(minCnt, MeasureTransactionsSingle(client, endpoint));
				}

				Console.WriteLine("Minimum transaction count: " + minCnt);
			}
		}

		private static int MeasureTransactionsSingle(IRestClient client, string endpoint)
		{
			Console.Write(endpoint);
			var cntPre = GetTransactionCnt(CountQuery);
			client.Get(endpoint);
			var cntPost = GetTransactionCnt(CountQuery);
			var cntDiff = cntPost - cntPre;
			Console.WriteLine("\tTransaction diff: " + cntDiff);
			return cntDiff;
		}


		private static int GetTransactionCnt(NpgsqlCommand command)
		{
			using (var reader = command.ExecuteReader())
			{
				while (reader.Read())
				{
					return reader.GetInt32(0);
				}
			}

			throw new Exception("Empty DB read.");
		}
	}
}
