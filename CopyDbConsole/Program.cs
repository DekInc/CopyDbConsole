using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CopyDbConsole
{
    class Program
    {
        static void Main(string[] args)
        {
            List<string> ListVerifExistTables = new List<string>();
            List<string> ListVerifExistTablesFields = new List<string>();
            List<string> ListTables = new List<string>();
            Dictionary<string, string> ListTableFields = new Dictionary<string, string>();
            List<Tuple<string, string>> ListData = new List<Tuple<string, string>>();
            Console.WriteLine("Escriba la conexión de origen, formato Server={};Database={};Connection Timeout=60;User Id={};Password={};");
            string OriginCon = Console.ReadLine();
            Console.WriteLine("Escriba la conexión de destino, formato Server={};Database={};Connection Timeout=60;User Id={};Password={};");
            string DestCon = Console.ReadLine();
            SqlConnection OriginSqlCon = new SqlConnection(OriginCon);
            SqlConnection DestSqlCon = new SqlConnection(DestCon);
            Console.WriteLine("PASO 1 - VERIFICANDO INTEGRIDAD DE LAS TABLAS");
            Console.WriteLine("Probando conexiones");
            OriginSqlCon.Open();
            DestSqlCon.Open();
            OriginSqlCon.Close();
            DestSqlCon.Close();
            Console.WriteLine("Construyendo schema origen");
            OriginSqlCon.Open();
            DataTable DtSchemaOrigen = OriginSqlCon.GetSchema("Tables");
            OriginSqlCon.Close();
            Console.WriteLine("Construyendo schema destino");
            DestSqlCon.Open();
            DataTable DtSchemaDest = DestSqlCon.GetSchema("Tables");
            DestSqlCon.Close();
            Console.WriteLine("Verificando que existan las mismas tablas");
            foreach (DataRow dataRow in DtSchemaOrigen.Rows)
            {
                if (dataRow["TABLE_TYPE"].ToString() == "BASE TABLE")
                {
                    string TableName = dataRow["TABLE_NAME"].ToString();
                    ListTables.Add(TableName);
                    Console.WriteLine($"Verificando existencia de tabla \t{TableName} \ten destino");
                    DataRow[] foundRows = DtSchemaDest.Select($"TABLE_NAME='{TableName}'");
                    if (foundRows.Length == 0)
                    {
                        Console.WriteLine($"La tabla {TableName} no se ha encontrado en el destino");
                        Console.ReadLine();
                        return;
                    }
                    else {
                        ListVerifExistTables.Add($"La tabla \t{TableName} \texiste en el destino");
                    }
                }
            }
            ListVerifExistTables.ForEach(T => Console.WriteLine(T));
            Console.WriteLine("VERIFICANDO QUE LAS TABLAS TENGAN LOS MISMOS CAMPOS Y CONSTRUYENDO DATOS A COPIAR, PUEDE TARDAR");            
            foreach (string TableName in ListTables) {
                ListTableFields.Add(TableName, "");
                OriginSqlCon.Open();
                DestSqlCon.Open();
                SqlCommand CmdOr = new SqlCommand($"SELECT * FROM {TableName}", OriginSqlCon);
                SqlDataReader DrOrigin = CmdOr.ExecuteReader();
                SqlCommand CmdDest = new SqlCommand($"SELECT TOP 1 * FROM {TableName}", DestSqlCon);
                SqlDataReader DrDest = CmdDest.ExecuteReader();
                if (DrOrigin.FieldCount != DrDest.FieldCount) {
                    Console.WriteLine($"No existe el mismo número de campos en la tabla {TableName}");
                    DrOrigin.Close();
                    DrDest.Close();
                    OriginSqlCon.Close();
                    DestSqlCon.Close();
                    return;
                }
                DataTable DtFieldsOrigin = DrOrigin.GetSchemaTable();
                DataTable DtFieldsDest = DrOrigin.GetSchemaTable();
                for(int Ci = 0; Ci < DtFieldsOrigin.Rows.Count; Ci++)
                {
                    if (DtFieldsOrigin.Rows[Ci][0].ToString() != DtFieldsDest.Rows[Ci][0].ToString())
                    {
                        Console.WriteLine($"No existe el campo {DtFieldsOrigin.Rows[Ci][0].ToString()} en la tabla {TableName} en la bd de destino");
                        DrOrigin.Close();
                        DrDest.Close();
                        OriginSqlCon.Close();
                        DestSqlCon.Close();
                        return;
                    }
                    else
                    {
                        if (DtFieldsOrigin.Rows[Ci][0].ToString().ToLower() != "id")
                        {
                            ListTableFields[TableName] += $"{DtFieldsOrigin.Rows[Ci][0].ToString().ToLower()}, ";
                        }
                    }
                }
                ListTableFields[TableName] = ListTableFields[TableName].Substring(0, ListTableFields[TableName].Length - 2);
                ListVerifExistTablesFields.Add($"Los campos de la tabla \t{TableName} \tson iguales");
                string Datos = string.Empty;
                while (DrOrigin.Read())
                {
                    Datos = string.Empty;
                    for (int Ci = 0; Ci < DtFieldsOrigin.Rows.Count; Ci++)
                    {
                        if (DtFieldsOrigin.Rows[Ci][0].ToString().ToLower() != "id")
                        {
                            Datos += $"'{DrOrigin.GetValue(Ci).ToString().Replace("'", "''")}', ";
                        }
                    }
                    Datos = Datos.Substring(0, Datos.Length - 2);
                    ListData.Add(new Tuple<string, string>(TableName, Datos));
                }                
                DrOrigin.Close();
                DrDest.Close();
                OriginSqlCon.Close();
                DestSqlCon.Close();
            }
            ListVerifExistTablesFields.ForEach(T => Console.WriteLine(T));
            Console.WriteLine("PASO 2 - COPIANDO LOS DATOS DE ORIGEN HACIA EL DESTINO");
            Console.WriteLine("Borrando destino");
            foreach (string TableName in ListTables)
            {
                DestSqlCon.Open();
                SqlCommand CmdDest = new SqlCommand($"TRUNCATE TABLE {TableName}", DestSqlCon);
                CmdDest.ExecuteNonQuery();                
                DestSqlCon.Close();
            }
            foreach (string TableName in ListTables)
            {
                //if (TableName != "EdiComs")
                //{
                    Console.WriteLine("Insertando datos de tabla " + TableName);
                    DestSqlCon.Open();
                    foreach (Tuple<string, string> Data in ListData.Where(D => D.Item1 == TableName))
                    {
                        SqlCommand CmdDest = new SqlCommand($"INSERT INTO {TableName}({ListTableFields[TableName]}) VALUES ({Data.Item2})", DestSqlCon);
                        CmdDest.ExecuteNonQuery();
                    }
                    DestSqlCon.Close();
                //}
            }
            Console.WriteLine("Presione una tecla para terminar");
            Console.ReadLine();
        }
    }
}
