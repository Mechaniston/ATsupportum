// copyright Clasix Mech_SQL 05.10.2015
function  sql_CreateConnection(sqlServer, sqlDB, sqlAuthWindows = true, sqlLogin = "", sqlPass = "",
 sqlProvider = "SQLOLEDB", sqlConnectionTimeout = 15, sqlCommandTimeout = 30, sqlCursorOnClient = false) export
	
	sqlConnection = new ComObject("ADODB.Connection");
	sqlConnection.Provider = sqlProvider;
	sqlConnection.ConnectionTimeOut = sqlConnectionTimeout;
	sqlConnection.CommandTimeOut = sqlCommandTimeout;
	if sqlCursorOnClient then
		sqlConnection.CursorLocation = 3;
	else
		sqlConnection.CursorLocation = 2;
	endif;
	
	sqlConnection.Properties("Data Source").Value = sqlServer;
	sqlConnection.Properties("Initial Catalog").Value = sqlDB;
	
	if sqlAuthWindows then
		sqlConnection.Properties("Integrated Security").Value = "SSPI";
	else
		sqlConnection.Properties("User ID").Value = sqlLogin;
		sqlConnection.Properties("Password").Value = sqlPass;
	endif;
	
	return sqlConnection;
	
endfunction

function  sql_Connect(sqlConnection) export
	
	if sqlConnection.State = 0 then // closed
		try
			SavedData = SaveConnectionProperties(sqlConnection);
			sqlConnection.Open();
		except
			RestoreConnectionProperties(sqlConnection, SavedData); //а оно нужно-правильно?
			Сообщить("Ошибка подключения к БД <" + sqlConnection.Properties("Initial Catalog").Value
				+ "> на SQL сервере <" + sqlConnection.Properties("Data Source").Value + ">: "
				+ ОписаниеОшибки(), СтатусСообщения.Важное);
			return False;
		endtry;
	endif;
	
	return (sqlConnection.State = 1); // opened
	
endfunction

function  sql_Close(sqlObj) export
	
	if sqlObj.State = 1 then // opened
		try
			sqlObj.Close();
		except
			return False;
		endtry;
	endif;
	
	IsClosed = (sqlObj.State = 0); // closed
	
	sqlObj = undefined;
	return IsClosed;

endfunction
 
function  sql_Execute(sqlConnection, Query, CloseAfterExecute = false) export
	
	if not sql_Connect(sqlConnection) then
		return false;
	endif;
	
	Result = false;
	
	try
		sqlConnection.BeginTrans();
		sqlConnection.Execute(TrimAll(Query), 0, 128);
// 2nd parameter:
//The optional RecordsAffected parameter is a long variable in which the provider stores
//the number of records that were affected by the query.
//
// 3rd parameter:
//The optional Options parameter defines how the provider should evaluate the CommandText parameter.
//It is a long value that is one or more of the CommandTypeEnum or ExecuteOptionEnum constants.
// CommandTypeEnum Constants:
//Constant 			Value 	Description 
//adCmdUnspecified 	-1 		Default, does not specify how to evaluate 
//adCmdText 		1 		Evaluate as a textual definition 
//adCmdTable 		2 		Have the provider generate a SQL query and return all rows from the specified table 
//adCmdStoredProc 	4 		Evaluate as a stored procedure 
//adCmdUnknown 		8 		The type of the CommandText parameter is unknown 
//adCmdFile 		256 	Evaluate as a previously persisted file 
//adCmdTableDirect 	512 	Return all rows from the specified table 
// ExecuteOptionEnum Constants:
//Constant 					Value 	Description 
//adOptionUnspecified 		-1 		The option parameter is unspecified 
//adAsyncExecute 			16 		Execute asynchronously 
//adAsyncFetch 				32 		Rows beyond the initial quantity specified should be fetched asynchronously 
//adAsyncFetchNonBlocking 	64 		Records are fetched asynchronously with no blocking of additional operations 
//adExecuteNoRecords 		128 	Does not return rows and must be combined with adCmdText or adCmdStoredProc 

		sqlConnection.CommitTrans();
		Result = true;
	except
		sqlConnection.RollBackTrans();
		Сообщить("Ошибка операции с БД <" + sqlConnection.Properties("Initial Catalog").Value
			+ "> на SQL сервере <" + sqlConnection.Properties("Data Source").Value + ">: "
			+ ОписаниеОшибки() + Символы.ПС + "  Текст запроса:" + Символы.ПС + Query, СтатусСообщения.Важное);
	endtry;
	
	if CloseAfterExecute then
		sql_Close(sqlConnection);
	endif;
	
	return Result;
	
endfunction

function  sql_GetQueryResult(sqlConnection, Query, VTb = undefined, CloseAfterExecute = false) export
	
	if not sql_Connect(sqlConnection) then
		return false;
	endif;
	
	if VTb = undefined then
		VTb = new ValueTable();
	else
		VTb.Clear();
	endif;
	
	Result = False;
	
	sqlRecordSet = new ComObject("ADODB.RecordSet");
	try
		sqlRecordSet.Open(TrimAll(Query), sqlConnection);
// 3rd parameter:			
//The optional CursorType parameter is one of the CursorTypeEnum constants that specifies the type of cursor to use when you open a Recordset object.
//CursorTypeEnum Constants:
//Constant 			Value 	Description
//adOpenUnspecified 	-1 		Cursor type not specified
//adOpenForwardOnly 	0 		Default, a forward scrolling only, static cursor where changes made by other users are not visible
//adOpenDynamic 		2 		A dynamic cursor with both forward and backward scrolling where additions, deletions, insertions, and updates made by other users are visible
//adOpenKeyset 			1 		A keyset cursor allows you to see dynamic changes to a specific group of records but you cannot see new records added by other users
//adOpenStatic 			3 		A static cursor allowing forward and backward scrolling of a fixed, unchangeable set of records
//
// 4th parameter:			
//The optional LockType parameter is one of the LockTypeEnum constants that indicates the type of lock in effect on a Recordset.
//LockTypeEnum Constants:
//Constant 				Value 	Description
//adLockUnspecified 		-1 		Lock type unknown 
//adLockReadOnly 			1 		Default, a read-only data 
//adLockPessimistic 		2 		The provider locks each record before and after you edit, and prevents other users from modifying the data 
//adLockOptimistic 			3 		Multiple users can modify the data which is not locked until Update is called 
//adLockBatchOptimistic 	4 		Multiple users can modify the data and the changes are cached until BatchUpdate is called 

		if VTb.Columns.Count() = 0 then
			for i = 0 to sqlRecordSet.Fields.Count do
				VTb.Columns.Add();
			enddo;
		endif;
		
		if sqlRecordSet.State = 1 then // open
			while not sqlRecordSet.EOF do
				NewVTbLine = VTb.Add();
				for i = 0 to Min(VTb.Columns.Count(), sqlRecordSet.Fields.Count) - 1 do
					NewVTbLine[i] = sqlRecordSet.Fields(i).Value;
				enddo;
				sqlRecordSet.MoveNext();
			enddo;
		endif;
		
		Result = True;
	except
		Сообщить("Ошибка операции с БД <" + sqlConnection.Properties("Initial Catalog").Value
			+ "> на SQL сервере <" + sqlConnection.Properties("Data Source").Value + ">: "
			+ ОписаниеОшибки() + Символы.ПС + "  Текст запроса:" + Символы.ПС + Query, СтатусСообщения.Важное);
			
	endtry;
	
	sql_Close(sqlRecordSet);
	
	if CloseAfterExecute then
		sql_Close(sqlConnection);
	endif;
	
	return Result;
	
endfunction

function  sql_QueryProcessing(sqlConnection, Query, Code, Param = "", CloseAfterExecute = false) export
	
	if not sql_Connect(sqlConnection) then
		return false;
	endif;
	
	Result = False;
	
	sqlRecordSet = new ComObject("ADODB.RecordSet");
	try
		sqlRecordSet.Open(TrimAll(Query), sqlConnection);
		
		BreakProcessing = False;
		
		while not sqlRecordSet.EOF do
			try
				execute(Code);
			except
			endtry;
			if BreakProcessing then
				break;
			endif;
			sqlRecordSet.MoveNext();
		enddo;
		
		Result = not BreakProcessing;
	except
		Сообщить("Ошибка операции с БД <" + sqlConnection.Properties("Initial Catalog").Value
			+ "> на SQL сервере <" + sqlConnection.Properties("Data Source").Value + ">: "
			+ ОписаниеОшибки() + Символы.ПС + "  Текст запроса:" + Символы.ПС + Query, СтатусСообщения.Важное);
	endtry;
	
	sql_Close(sqlRecordSet);
	
	if CloseAfterExecute then
		sql_Close(sqlConnection);
	endif;
	
	return Result;
	
endfunction

function  sql_GetNumParamStr(Value) export
	
	if TypeOf(Value) = Type("Number") then
		return String(Value);
	else
		return String(Number(Value));
	endif;
	
endfunction

function  sql_GetDateTimeParamStr(Value) export
	
	return "'" + Format(Value, "ДФ='yyyy/MM/dd HH:mm:ss'") + "'";
	
endfunction 

function  sql_GetStrParamStr(Value) export
	
	return "'" + StrReplace(TrimAll(String(Value)), "'", "''") + "'";
	
endfunction 


//// Internal

function  SaveConnectionProperties(sqlConnection)
	
	SavedData = new Structure("Provider,ConnectionTimeOut,CommandTimeOut,CursorLocation,
		|DataSource,InitialCatalog,IntegratedSecurity,UserID,Password");
		
	SavedData.Provider = sqlConnection.Provider;
	SavedData.ConnectionTimeout = sqlConnection.ConnectionTimeOut;
	SavedData.CommandTimeout = sqlConnection.CommandTimeOut;
	SavedData.CursorLocation = sqlConnection.CursorLocation;
	SavedData.DataSource = sqlConnection.Properties("Data Source").Value;
	SavedData.InitialCatalog = sqlConnection.Properties("Initial Catalog").Value;
	SavedData.IntegratedSecurity = sqlConnection.Properties("Integrated Security").Value;
	SavedData.UserID = sqlConnection.Properties("User ID").Value;
	SavedData.Password = sqlConnection.Properties("Password").Value;
	
	return SavedData;
	
endfunction

procedure RestoreConnectionProperties(sqlConnection, SavedData)
	
	sqlConnection.Provider = SavedData.Provider;
	sqlConnection.ConnectionTimeOut = SavedData.ConnectionTimeout;
	sqlConnection.CommandTimeOut = SavedData.CommandTimeout;
	sqlConnection.CursorLocation = SavedData.CursorLocation;
	sqlConnection.Properties("Data Source").Value = SavedData.DataSource;
	sqlConnection.Properties("Initial Catalog").Value = SavedData.InitialCatalog;
	sqlConnection.Properties("Integrated Security").Value = SavedData.IntegratedSecurity;
	sqlConnection.Properties("User ID").Value = SavedData.UserID;
	sqlConnection.Properties("Password").Value = SavedData.Password;
	
endprocedure
