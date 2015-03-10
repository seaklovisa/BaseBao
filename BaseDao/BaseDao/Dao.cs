using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data.Sql;
using System.Data;
using System.Data.SqlClient;
namespace BaseDao
{
    public class Dao
    {
        System.Data.SqlClient.SqlConnection conn = null;
        string connStr = "";
        System.Data.SqlClient.SqlTransaction tran;

        public Dao(string connStr)
        {
            this.connStr = connStr;
        }

        public System.Data.SqlClient.SqlDataReader GetDataReader(string sql)
        {
            try
            {
                CreateConnection();
                System.Data.SqlClient.SqlCommand sm = new SqlCommand();

                sm.Parameters.Clear();
                sm.CommandText = sql;
                sm.CommandType = CommandType.Text;
                sm.Connection = conn;
                sm.Transaction = tran;
                SqlDataReader sr = sm.ExecuteReader();
                return sr;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (tran == null)
                    CloseConnection();
            }
        }

        public System.Data.DataSet GetDataSet(string sql)
        {
            try
            {
                CreateConnection();
                System.Data.SqlClient.SqlCommand sm = new SqlCommand();
                sm.Connection = conn;
                sm.Transaction = tran;
                sm.CommandText = sql;
                sm.CommandType = CommandType.Text;
                System.Data.DataSet ds = new DataSet();
                System.Data.SqlClient.SqlDataAdapter adp = new SqlDataAdapter(sm);
                adp.Fill(ds);
                return ds;
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (tran == null)
                    CloseConnection();
            }
        }

        public string GetSclar(string sql)
        {
            try
            {
                CreateConnection();
                System.Data.SqlClient.SqlCommand sm = new SqlCommand();
                sm.Connection = conn;
                sm.Transaction = tran;
                sm.CommandText = sql;
                sm.CommandType = CommandType.Text;
                return sm.ExecuteScalar().ToString();
                
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally{
                if (tran == null)
                    CloseConnection();
            }
        }

        public void CreateConnection()
        {
            try
            {
                if (conn == null || conn.State == System.Data.ConnectionState.Closed)
                {
                    conn = new SqlConnection(connStr);
                    conn.Open();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void BeginTransacation()
        {
            try
            {
                CreateConnection();
                tran = conn.BeginTransaction();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void CloseConnection()
        {
            try
            {
                if (conn != null || conn.State == System.Data.ConnectionState.Open)
                {
                    conn.Close();
                    conn.Dispose();
                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                conn = null;
            }
        }

        public void CommitTrans()
        {
            try
            {
                if (tran == null)
                {
                    throw new Exception("Transacation is null");
                }
                tran.Commit();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public void RollBackTrans()
        {
            try
            {
                if (tran == null)
                {
                    throw new Exception("Transacation is null");
                }
                tran.Rollback();
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        public int ExecuteSqlCommand(string sql)
        {
            try
            {

                CreateConnection();
                System.Data.SqlClient.SqlCommand sm = new SqlCommand();
                sm.Parameters.Clear();
                sm.Connection = conn;
                sm.Transaction = tran;
                sm.CommandText = sql;
                sm.CommandType = System.Data.CommandType.Text;

                return sm.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw new Exception(ex.Message + " - " + sql);
            }
            finally
            {
                if (tran == null)
                    CloseConnection();
            }
        }

        public int ExecuteSqlCommand(string sql, SqlCommand sm)
        {
            try
            {
                CreateConnection();
                sm.CommandText = sql;
                sm.CommandType = CommandType.Text;
                sm.Connection = conn;
                sm.Transaction = tran;
                return sm.ExecuteNonQuery();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (tran == null)
                    CloseConnection();
            }
        }
    }
}
