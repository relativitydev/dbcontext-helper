using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using kCura.Relativity.Client;
using kCura.Relativity.Client.DTOs;

namespace DBContextHelper.Tests.Integration.Helpers
{
	public class ArtifactHelpers
	{

		public static int CreateWorkspace(IRSAPIClient proxy, string workspaceName, string templateName)
		{
			var oldWorkspaceId = proxy.APIOptions.WorkspaceID;
			try
			{
				proxy.APIOptions.WorkspaceID = -1;

				if (string.IsNullOrWhiteSpace(templateName))
				{
					throw new SystemException(
						"Template name is blank in your configuration setting. Please add a template name to create a workspace");
				}


				var resultSet = GetArtifactIdOfTemplate(proxy, templateName);

				if (!resultSet.Success)
				{
					throw new ApplicationException($"Error creating workspace {workspaceName} with error {resultSet.Message}");
				}

				if (!resultSet.Results.Any())
				{
					throw new ApplicationException($"No template with name {templateName} found in this environment");
				}

				var workspace = resultSet.Results.FirstOrDefault().Artifact;
				int templateArtifactID = workspace.ArtifactID;

				var workspaceDTO = new Workspace();
				workspaceDTO.Name = workspaceName;

				//if (serverId.HasValue)
				//{
				//	workspaceDTO.ServerID = serverId.Value;
				//	}

				var result = proxy.Repositories.Workspace.CreateAsync(templateArtifactID, workspaceDTO);

				if (!result.Success)
				{
					throw new System.Exception($"Workspace creation failed: {result.Message}");
				}

				ProcessInformation info = proxy.GetProcessState(proxy.APIOptions, result.ProcessID);

				int iteration = 0;

				//I have a feeling this will bite us in the future, but it hasn't yet
				while (info.State != ProcessStateValue.Completed)
				{
					//Workspace creation takes some time sleep until the workspaces is created and then get the artifact id of the new workspace
					System.Threading.Thread.Sleep(10000);
					info = proxy.GetProcessState(proxy.APIOptions, result.ProcessID);

					if (iteration > 6)
					{
						Console.WriteLine("Workspace creation timed out");
					}

					iteration++;
				}

				var testId = info?.OperationArtifactIDs?.FirstOrDefault();
				if (!testId.HasValue)
				{
					throw new Exception("There was an error getting the created workspaceId");
				}

				return testId.Value;
			}
			catch (Exception ex)
			{
				Console.WriteLine("Workspace creation failed" + ex.Message);
				throw ex;
			}
			finally
			{
				proxy.APIOptions.WorkspaceID = oldWorkspaceId;
			}
		}

		private static QueryResultSet<Workspace> GetArtifactIdOfTemplate(IRSAPIClient proxy, string templateName)
		{
			int? templateArtifactID = null;
			kCura.Relativity.Client.DTOs.Query<kCura.Relativity.Client.DTOs.Workspace> query = new kCura.Relativity.Client.DTOs.Query<kCura.Relativity.Client.DTOs.Workspace>();
			query.Condition = new kCura.Relativity.Client.TextCondition(kCura.Relativity.Client.DTOs.FieldFieldNames.Name, kCura.Relativity.Client.TextConditionEnum.EqualTo, templateName);
			query.Fields = kCura.Relativity.Client.DTOs.FieldValue.NoFields;
			kCura.Relativity.Client.DTOs.QueryResultSet<kCura.Relativity.Client.DTOs.Workspace> resultSet = proxy.Repositories.Workspace.Query(query, 0);

			return resultSet;
		}
	}
}
