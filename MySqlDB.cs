
using System.Data;
using MySql.Data.MySqlClient;

namespace Database_Access_Layer
{
    public class MySqlDB : IDbContext
    {
        public string _ConnectionString { get; set; }

        public MySqlDB(string connectionString)
        {
            _ConnectionString = connectionString;
        }

        public bool InsertUpdateDelete(string sql, List<DalDbParameter> parameters)
        {
            using (MySqlConnection mySqlConnection = new MySqlConnection(_ConnectionString))
            {
                mySqlConnection.Open();

                using (MySqlCommand mySqlCommand = new MySqlCommand(sql, mySqlConnection))
                {
                    mySqlCommand.CommandType = System.Data.CommandType.Text;

                    foreach (var parameter in parameters)
                        mySqlCommand.Parameters.Add(new MySqlParameter(parameter.Name, parameter.Value));

                    return mySqlCommand.ExecuteNonQuery() > 0;
                }
            }
        }

        public async Task<bool> InsertUpdateDeleteAsync(string sql, List<DalDbParameter> parameters)
        {
            using (MySqlConnection mySqlConnection = new MySqlConnection(_ConnectionString))
            {
                mySqlConnection.Open();

                using (MySqlCommand mySqlCommand = new MySqlCommand(sql, mySqlConnection))
                {
                    mySqlCommand.CommandType = System.Data.CommandType.Text;

                    foreach (var parameter in parameters)
                        mySqlCommand.Parameters.Add(new MySqlParameter(parameter.Name, parameter.Value));

                    return await mySqlCommand.ExecuteNonQueryAsync() > 0;
                }
            }
        }

        public DataTable Select(string sql)
        {
            using (MySqlConnection mySqlConnection = new MySqlConnection(_ConnectionString))
            {
                mySqlConnection.Open();

                using (MySqlCommand mySqlCommand = new MySqlCommand(sql, mySqlConnection))
                {
                    mySqlCommand.CommandType = System.Data.CommandType.Text;

                    using (MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter(mySqlCommand))
                    {
                        using (DataTable dt = new DataTable())
                        {
                            mySqlDataAdapter.Fill(dt);
                            return dt;
                        }
                    }

                }
            }
        }

        public async Task<DataTable> SelectAsync(string sql)
        {
            using (MySqlConnection mySqlConnection = new MySqlConnection(_ConnectionString))
            {
                mySqlConnection.Open();

                using (MySqlCommand mySqlCommand = new MySqlCommand(sql, mySqlConnection))
                {
                    mySqlCommand.CommandType = System.Data.CommandType.Text;

                    using (MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter(mySqlCommand))
                    {
                        using (DataTable dt = new DataTable())
                        {
                            await Task.Run(() => mySqlDataAdapter.Fill(dt));
                            return dt;
                        }
                    }

                }
            }
        }

        public DataTable Select(string sql, List<DalDbParameter> parameters)
        {
            using (MySqlConnection mySqlConnection = new MySqlConnection(_ConnectionString))
            {
                mySqlConnection.Open();

                using (MySqlCommand mySqlCommand = new MySqlCommand(sql, mySqlConnection))
                {
                    mySqlCommand.CommandType = System.Data.CommandType.Text;

                    if (parameters != null && parameters.Count > 0)
                    {
                        foreach (var parameter in parameters)
                            mySqlCommand.Parameters.Add(new MySqlParameter(parameter.Name, parameter.Value));
                    }

                    using (MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter(mySqlCommand))
                    {
                        using (DataTable dt = new DataTable())
                        {
                            mySqlDataAdapter.Fill(dt);
                            return dt;
                        }
                    }

                }
            }
        }

        public async Task<DataTable> SelectAsync(string sql, List<DalDbParameter> parameters)
        {
            using (MySqlConnection mySqlConnection = new MySqlConnection(_ConnectionString))
            {
                mySqlConnection.Open();

                using (MySqlCommand mySqlCommand = new MySqlCommand(sql, mySqlConnection))
                {
                    mySqlCommand.CommandType = System.Data.CommandType.Text;

                    if (parameters != null && parameters.Count > 0)
                    {
                        foreach (var parameter in parameters)
                            mySqlCommand.Parameters.Add(new MySqlParameter(parameter.Name, parameter.Value));
                    }

                    using (MySqlDataAdapter mySqlDataAdapter = new MySqlDataAdapter(mySqlCommand))
                    {
                        using (DataTable dt = new DataTable())
                        {
                            await Task.Run(() => mySqlDataAdapter.Fill(dt));
                            return dt;
                        }
                    }

                }
            }
        }

        public DataTable ExecuteProcedure(string storedProcedureName, List<DalDbParameter> parameters)
        {
            using (MySqlConnection sqlConnection = new MySqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (MySqlCommand sqlCommand = new MySqlCommand(storedProcedureName, sqlConnection))
                {
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    foreach (var parameter in parameters)
                    {
                        sqlCommand.Parameters.Add(new MySqlParameter() { ParameterName = parameter.Name, Value = parameter.Value });
                    }
                    MySqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
                    var dt = new DataTable();
                    dt.Load(sqlDataReader);

                    return dt;
                }
            }
        }

        public async Task<DataTable> ExecuteProcedureAsync(string storedProcedureName, List<DalDbParameter> parameters)
        {
            using (MySqlConnection sqlConnection = new MySqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (MySqlCommand sqlCommand = new MySqlCommand(storedProcedureName, sqlConnection))
                {
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    foreach (var parameter in parameters)
                    {
                        sqlCommand.Parameters.Add(new MySqlParameter() { ParameterName = parameter.Name, Value = parameter.Value });
                    }
                    MySqlDataReader sqlDataReader = (MySqlDataReader)await sqlCommand.ExecuteReaderAsync();
                    var dt = new DataTable();
                    dt.Load(sqlDataReader);

                    return dt;
                }
            }
        }

        public DataTable ExecuteProcedure(string storedProcedureName)
        {
            using (MySqlConnection sqlConnection = new MySqlConnection(_ConnectionString))
            {
                sqlConnection.Open();

                using (MySqlCommand sqlCommand = new MySqlCommand(storedProcedureName, sqlConnection))
                {
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    MySqlDataReader sqlDataReader = sqlCommand.ExecuteReader();
                    var dt = new DataTable();
                    dt.Load(sqlDataReader);

                    return dt;
                }
            }
        }

        public async Task<DataTable> ExecuteProcedureAsync(string storedProcedureName)
        {
            using (MySqlConnection sqlConnection = new MySqlConnection(_ConnectionString))
            {
                await sqlConnection.OpenAsync();

                using (MySqlCommand sqlCommand = new MySqlCommand(storedProcedureName, sqlConnection))
                {
                    sqlCommand.CommandType = CommandType.StoredProcedure;
                    MySqlDataReader sqlDataReader = (MySqlDataReader)await sqlCommand.ExecuteReaderAsync();
                    var dt = new DataTable();
                    dt.Load(sqlDataReader);

                    return dt;
                }
            }
        }

        public void BackupDatabase(string databaseName, string backupFolderPath)
        {
            string backupDateTime = DateTime.Now.ToString("yyyyMMddTHHmmss");
            string backupFilePath = Path.Combine(backupFolderPath, "Backup", $"{databaseName}_{backupDateTime}.sql");

            using (MySqlConnection conn = new MySqlConnection(_ConnectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand())
                {
                    using (MySqlBackup mb = new MySqlBackup(cmd))
                    {
                        cmd.Connection = conn;
                        conn.Open();
                        mb.ExportInfo.AddCreateDatabase = true;

                        mb.ExportToFile(backupFilePath);
                        conn.Close();
                    }
                }
            }
        }

        public async Task BackupDatabaseAsync(string databaseName, string backupFolderPath)
        {
            string backupDateTime = DateTime.Now.ToString("yyyyMMddTHHmmss");
            string backupFilePath = Path.Combine(backupFolderPath, "Backup", $"{databaseName}_{backupDateTime}.sql");

            using (MySqlConnection conn = new MySqlConnection(_ConnectionString))
            {
                using (MySqlCommand cmd = new MySqlCommand())
                {
                    using (MySqlBackup mb = new MySqlBackup(cmd))
                    {
                        cmd.Connection = conn;
                        conn.Open();
                        mb.ExportInfo.AddCreateDatabase = true;

                        await Task.Run(() => mb.ExportToFile(backupFilePath));
                        conn.Close();
                    }
                }
            }
        }
    }
}
