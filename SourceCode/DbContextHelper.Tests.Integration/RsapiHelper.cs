using kCura.Relativity.Client;
using kCura.Relativity.Client.DTOs;
using System;
using System.Linq;

namespace DBContextHelper.Tests.Integration
{
	public class RsapiHelper
	{
		public static IRSAPIClient CreateRsapiClient(Uri servicesUri, string userName, string password)
		{
			try
			{
				IRSAPIClient rsapiClient = new RSAPIClient(servicesUri, new UsernamePasswordCredentials(userName, password));
				return rsapiClient;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Failed to create a new instance of IRSAPIClient. ErrorMessage: {ex.Message}", ex);
				throw;
			}
		}

		public static int CreateWorkspace(IRSAPIClient rsapiClient, string workspaceName, string templateName)
		{
			int workspaceArtifactId = -100;

			try
			{
				rsapiClient.APIOptions.WorkspaceID = -1;

				// retry logic for workspace creation
				int j = 1;
				while (j < 5)
				{
					j++;
					try
					{
						Console.WriteLine("Creating workspace.....");

						workspaceArtifactId = AttemptWorkspaceCreation(rsapiClient, workspaceName, templateName);
						Console.WriteLine($"Workspace created [WorkspaceArtifactId= {workspaceArtifactId}].....");
						break;
					}
					catch (Exception e)
					{
						Console.WriteLine("Failed to create workspace, Retry now...");

						if (j != 5)
						{
							continue;
						}

						throw new Exception($"Failed to create workspace in the setup. Reset the Client to null\nError Message:\n{e.Message}.\nInner Exception Message:\n{e.InnerException?.Message}.\nStack trace:\n{e.StackTrace}.", e);
					}
				}
			}
			catch (Exception ex)
			{
				throw new Exception("Error encountered while creating a new Workspace.", ex);
			}

			return workspaceArtifactId;
		}

		private static int AttemptWorkspaceCreation(IRSAPIClient rsapiClient, string workspaceName, string templateName)
		{
			int oldWorkspaceId = rsapiClient.APIOptions.WorkspaceID;

			try
			{
				rsapiClient.APIOptions.WorkspaceID = -1;

				if (string.IsNullOrWhiteSpace(templateName))
				{
					throw new Exception("Template name is blank in your configuration setting. Please add a template name to create a workspace");
				}

				QueryResultSet<Workspace> templateWorkspaceQueryResultSet = GetArtifactIdOfTemplate(rsapiClient, templateName);

				if (!templateWorkspaceQueryResultSet.Success)
				{
					throw new ApplicationException($"Error creating workspace {workspaceName} with error {templateWorkspaceQueryResultSet.Message}");
				}

				if (!templateWorkspaceQueryResultSet.Results.Any())
				{
					throw new ApplicationException($"No template with name {templateName} found in this environment");
				}

				Workspace workspace = templateWorkspaceQueryResultSet.Results.FirstOrDefault()?.Artifact;
				if (workspace == null)
				{
					throw new Exception("Template Workspace is NULL");
				}

				int templateArtifactId = workspace.ArtifactID;

				Workspace workspaceDto = new Workspace
				{
					Name = workspaceName
				};

				ProcessOperationResult result = rsapiClient.Repositories.Workspace.CreateAsync(templateArtifactId, workspaceDto);

				if (!result.Success)
				{
					throw new Exception($"Workspace creation failed: {result.Message}");
				}

				ProcessInformation info = rsapiClient.GetProcessState(rsapiClient.APIOptions, result.ProcessID);

				int iteration = 0;

				//I have a feeling this will bite us in the future, but it hasn't yet
				while (info.State != ProcessStateValue.Completed)
				{
					//Workspace creation takes some time sleep until the workspaces is created and then get the artifact id of the new workspace
					System.Threading.Thread.Sleep(10000);
					info = rsapiClient.GetProcessState(rsapiClient.APIOptions, result.ProcessID);

					if (iteration > 6)
					{
						Console.WriteLine("Workspace creation timed out");
					}

					iteration++;
				}

				int? newWorkspaceArtifactId = info.OperationArtifactIDs?.FirstOrDefault();
				if (!newWorkspaceArtifactId.HasValue)
				{
					throw new Exception("There was an error getting the created workspaceId");
				}

				return newWorkspaceArtifactId.Value;
			}
			catch (Exception ex)
			{
				Console.WriteLine($"Workspace creation failed. ErrorMessage: {ex.Message}", ex);
				throw;
			}
			finally
			{
				rsapiClient.APIOptions.WorkspaceID = oldWorkspaceId;
			}
		}

		private static QueryResultSet<Workspace> GetArtifactIdOfTemplate(IRSAPIClient proxy, string templateName)
		{
			Query<Workspace> workspaceQuery = new Query<Workspace>
			{
				Condition = new TextCondition(FieldFieldNames.Name, TextConditionEnum.EqualTo, templateName),
				Fields = FieldValue.NoFields
			};
			QueryResultSet<Workspace> workspaceQueryResultSet = proxy.Repositories.Workspace.Query(workspaceQuery);

			return workspaceQueryResultSet;
		}

		public static void DeleteWorkspace(IRSAPIClient rsapiClient, int workspaceArtifactId)
		{
			try
			{
				rsapiClient.APIOptions.WorkspaceID = -1;
				rsapiClient.Repositories.Workspace.DeleteSingle(workspaceArtifactId);
			}
			catch (Exception ex)
			{
				Console.WriteLine($"An error occurred deleting the Workspace. ErrorMessage: {ex.Message}", ex);
			}
		}
	}
}
