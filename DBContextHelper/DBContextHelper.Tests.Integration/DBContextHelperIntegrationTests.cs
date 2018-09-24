using kCura.Relativity.Client;
using kCura.Relativity.Client.DTOs;
using NUnit.Framework;
using Relativity.API;
using Relativity.Test.Helpers;
using Relativity.Test.Helpers.ServiceFactory.Extentions;
using Relativity.Test.Helpers.SharedTestHelpers;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;

namespace DbContextHelper.Tests.Integration
{
	[TestFixture]
	public class DbContextHelperIntegrationTests
	{
		private string _sql;
		private IServicesMgr _servicesManager;
		private IRSAPIClient _client;
		private int _workspaceId;
		private string _workspaceName = "DBContext Helper";
		private string _workspaceNameChange = "DBContext Utility";

		public DbContext sut { get; set; }


		[SetUp]
		public void Setup()
		{
			//Setup for testing		
			TestHelper helper = new TestHelper();
			_servicesManager = helper.GetServicesManager();

			// implement_IHelper
			//create client
			_client = helper.GetServicesManager().GetProxy<IRSAPIClient>(ConfigurationHelper.ADMIN_USERNAME, ConfigurationHelper.DEFAULT_PASSWORD);

			sut = new DbContext(Relativity.Test.Helpers.SharedTestHelpers.ConfigurationHelper.SQL_SERVER_ADDRESS, string.Format("EDDS"), Relativity.Test.Helpers.SharedTestHelpers.ConfigurationHelper.SQL_USER_NAME, Relativity.Test.Helpers.SharedTestHelpers.ConfigurationHelper.SQL_PASSWORD);

		}

		[Test]
		public void ExecuteSqlStatementAsDataTable_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			DataTable dt = sut.ExecuteSqlStatementAsDataTable(_sql, 30, null);

			//Assert
			Assert.IsNotNull(dt);

			//Cleanup
			_sql = "";

		}

		[Test]
		public void ExecuteSqlStatementAsScalar_T_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT Count(*)  FROM [EDDS].[eddsdbo].[Case]");

			//Act
			int value = sut.ExecuteSqlStatementAsScalar<int>(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);
			Assert.Greater(value, 1);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSqlStatementAsScalar_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT Count(*)  FROM [EDDS].[eddsdbo].[Case]");

			//Act
			object value = sut.ExecuteSqlStatementAsScalar(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);
			Assert.Greater(Convert.ToInt32(value), 1);

			//Cleanup
			_sql = "";
		}

		/// <summary>
		/// This sql statement here is making an update to your database. PLEASE USE WITH CAUTION!!
		/// </summary>

		[Test]
		public void ExecuteNonQuerySQLStatement_Valid_sqlstring()
		{
			//Arrange
			_workspaceId = CreateWorkspace();
			_sql = String.Format(@"update [EDDS].[eddsdbo].[Case] Set Name = '{0}' where ArtifactID = '{1}'", _workspaceNameChange, _workspaceId);
			string _sql2 = String.Format(@"SELECT Name  FROM [EDDS].[eddsdbo].[Case] where ArtifactID = {0} ", _workspaceId);

			//Act
			sut.ExecuteNonQuerySQLStatement(_sql, null, 30);
			object value = sut.ExecuteSqlStatementAsScalar(_sql2, null, 30);

			//Assert
			Assert.AreEqual(value.ToString(), _workspaceNameChange);

			//Cleanup
			_sql = "";
			DeleteWorkspace(_client, _workspaceId);
		}

		[Test]
		public void ExecuteSqlStatementAsDbDataReader_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			DbDataReader value = sut.ExecuteSqlStatementAsDbDataReader(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSQLStatementGetSecondDataTable_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[User]; SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			DataTable value = sut.ExecuteSQLStatementGetSecondDataTable(_sql, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSQLStatementAsReader_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			SqlDataReader value = sut.ExecuteSQLStatementAsReader(_sql, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}


		[Test]
		public void ExecuteParameterizedSQLStatementAsReader_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			SqlDataReader value = sut.ExecuteParameterizedSQLStatementAsReader(_sql, null, 30);

			//Assert
			Assert.IsNotNull(value);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSQLStatementAsEnumerable_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT [ArtifactID], [Name] FROM [EDDS].[eddsdbo].[Case]");

			//Act
			IEnumerable<string> value = sut.ExecuteSQLStatementAsEnumerable<string>(_sql, ConvertToString, 30);

			//Assert
			Assert.IsNotNull(value);
			//todo: Add Assert to loop through the reader and test the values.

			//Cleanup
			_sql = "";
		}

		private string ConvertToString(SqlDataReader sqlDataReader)
		{
			string returnString = sqlDataReader.GetInt32(0).ToString();
			return returnString;
		}

		[Test]
		public void ExecuteProcedureAsReader_Valid_sqlstring()
		{
			//Arrange
			//Act
			SqlParameter param1 = new SqlParameter();
			param1.ParameterName = "@UserID";
			param1.SqlDbType = SqlDbType.Int;
			param1.Direction = ParameterDirection.Input;
			param1.Value = 9;


			SqlParameter param2 = new SqlParameter();
			param2.ParameterName = "@ArtifactID";
			param2.SqlDbType = SqlDbType.Int;
			param2.Direction = ParameterDirection.Input;
			param2.Value = 9;

			DbDataReader dbDataReader = sut.ExecuteProcedureAsReader("GetUserPermissions", new List<SqlParameter> { param1, param2 });

			//Assert
			Assert.IsNotNull(dbDataReader);

			//Cleanup
			_sql = "";
		}

		[Test]
		public void ExecuteSqlStatementAsDataSet_Valid_sqlstring()
		{
			//Arrange
			_sql = String.Format(@"SELECT * FROM [EDDS].[eddsdbo].[Case]");

			//Act
			DataSet dt = sut.ExecuteSqlStatementAsDataSet(_sql, null, 30);

			//Assert
			Assert.IsNotNull(dt);

			//Cleanup
			_sql = "";
		}

		#region Helpers
		private Int32 CreateWorkspace()
		{
			try
			{
				_client.APIOptions.WorkspaceID = -1;
				// retry logic for workspace creation
				int j = 1;

				while (j < 5)
				{
					j++;
					try
					{
						Console.WriteLine("Creating workspace.....");

						_workspaceId =
						Relativity.Test.Helpers.WorkspaceHelpers.CreateWorkspace.CreateWorkspaceAsync(_workspaceName, ConfigurationHelper.TEST_WORKSPACE_TEMPLATE_NAME, _servicesManager, ConfigurationHelper.ADMIN_USERNAME,
							ConfigurationHelper.DEFAULT_PASSWORD).Result;
						Console.WriteLine($"Workspace created [WorkspaceArtifactId= {_workspaceId}].....");
						j = 5;
					}
					catch (Exception e)
					{
						Console.WriteLine("Failed to create workspace, Retry now...");

						if (j != 5)
						{
							continue;
						}

						_client = null;
						throw new Exception(
							$"Failed to create workspace in the setup. Reset the Client to null\nError Message:\n{e.Message}.\nInner Exception Message:\n{e.InnerException.Message}.\nStrack trace:\n{e.StackTrace}.", e);
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error encountered while creating a new Workspace.", ex);
			}
			finally
			{
				Console.WriteLine($"Workspace created [WorkspaceArtifactId= {_workspaceId}].....");
			}
			return _workspaceId;
		}

		public static bool DeleteWorkspace(IRSAPIClient proxy, int workspaceID)
		{

			proxy.APIOptions.WorkspaceID = -1;
			try
			{
				//Create a Workspace Artifact and pass to the Delete method on the repository
				Workspace workspaceDTO = new kCura.Relativity.Client.DTOs.Workspace(workspaceID);
				proxy.Repositories.Workspace.DeleteSingle(workspaceID);

			}
			catch (Exception ex)
			{
				Console.WriteLine("An error occurred deleting the Workspace: {0}", ex.Message);
				return false;
			}
			return true;
		}

		#endregion
	}
}
