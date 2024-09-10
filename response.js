const fs = require('fs');
const path = require('path');
const queryDatabase = require('./dbQuery'); // Adjust the path to your actual dbQuery module

// Define the SQL query with a placeholder for calllist_id
const sqlQuery = `
  SELECT 
    calllist.tracking_cd as Tracking_code, 
    calllist.calllist_id as ICX_lead_id,
    calllist.target_id as Target_id,
    NULL as Refer_id,
    wrapup_code.wrapup_desc AS Resolution_Cd_1,
    CASE 
        WHEN wrapup_code.parent_code = 0 THEN NULL 
        ELSE parent_wrapup_code.wrapup_desc         
    END AS Resolution_Cd_2,
    call_status.hierarchy AS Resolution_Cd_Hierarchy,
    tapp.incident_status,
    tapp.quoteNo,
    tapp.agent_id as TSR,
    tapp.type_vouncher,
    tapp.vouncher_value,
    CASE
        WHEN calllist.personal_id IS NOT NULL THEN calllist.personal_id
        ELSE calllist.passport_no
    END AS NRIC,
    tapp.Dont_call_ind,
    tapp.Dont_SMS_ind,
    tapp.Dont_email_ind,
    tapp.Dont_Mail_ind,
    calllist.udf1, 
    calllist.udf2, 
    calllist.udf3, 
    calllist.udf4, 
    calllist.udf5, 
    calllist.udf6, 
    calllist.udf7, 
    calllist.udf8, 
    calllist.udf9, 
    calllist.udf10, 
    calllist.udf11, 
    calllist.udf12, 
    calllist.udf13, 
    calllist.udf14, 
    calllist.udf15, 
    calllist.udf16, 
    calllist.udf17, 
    calllist.udf18, 
    calllist.udf19, 
    calllist.udf20,
    call_trans.number_of_call AS call_attempt,
    DATE_FORMAT(call_trans.create_date, '%Y/%m/%d %H:%i:%s') AS call_start,
    DATE_FORMAT(call_trans.update_date, '%Y/%m/%d %H:%i:%s') AS call_end,
    CASE 
        WHEN RIGHT(call_trans.wrapup_id, 1) = '|' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(call_trans.wrapup_id, '|', -2), '|', 1)
        ELSE SUBSTRING_INDEX(call_trans.wrapup_id, '|', -1)
    END AS call_attempt_code,
    TIMESTAMPDIFF(second, call_trans.create_date, call_trans.update_date) AS duration_call,
    call_trans.voice_id,
    DATE_FORMAT(tapp.policy_create_date, '%Y/%m/%d %H:%i:%s') AS sales_date,
    DATE_FORMAT(tapp.propDate, '%Y/%m/%d %H:%i:%s') AS creation_date,
    DATE_FORMAT(tapp.update_date, '%Y/%m/%d %H:%i:%s') AS last_update_date
  FROM 
    icx_sg.t_calllist calllist
  INNER JOIN 
    t_aig_app tapp ON calllist.calllist_id = tapp.calllist_id 
  INNER JOIN 
    t_agents agent ON tapp.agent_id = agent.agent_id  
  INNER JOIN 
    t_call_trans call_trans ON calllist.calllist_id = call_trans.calllist_id 
  INNER JOIN 
    t_wrapup_code wrapup_code ON 
      CASE 
        WHEN RIGHT(call_trans.wrapup_id, 1) = '|' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(call_trans.wrapup_id, '|', -2), '|', 1)
        ELSE SUBSTRING_INDEX(call_trans.wrapup_id, '|', -1)
      END = wrapup_code.wrapup_code
  LEFT JOIN 
    t_wrapup_code parent_wrapup_code ON wrapup_code.parent_code = parent_wrapup_code.wrapup_code
  INNER JOIN 
    t_call_status call_status ON call_status.hierarchy = wrapup_code.call_status_id
  WHERE 
    calllist.calllist_id = ?
`;
const sqlQueryResponseCall = `
  SELECT 
    calllist.target_id as Target_id,
    calllist.calllist_id as ICX_lead_id,
    case
   		when calllist.udf1 is not null then calllist.udf1 -- mobile_tel
   		when calllist.udf2 is not null then calllist.udf2 -- home tel
   		when calllist.udf3 is not null then calllist.udf3 -- work tel
   		else null
   	end as Phone_no,
    call_trans.number_of_call as call_attempt,
    DATE_FORMAT(call_trans.create_date, '%Y/%m/%d %H:%i:%s') AS call_start,
    DATE_FORMAT(call_trans.update_date, '%Y/%m/%d %H:%i:%s') AS call_end,
    CASE 
        WHEN RIGHT(call_trans.wrapup_id , 1) = '|' THEN SUBSTRING_INDEX(SUBSTRING_INDEX(call_trans.wrapup_id, '|', -2), '|', 1)
        ELSE SUBSTRING_INDEX(call_trans.wrapup_id, '|', -1)
    END AS call_attempt_code,
    TIMESTAMPDIFF(second, call_trans.create_date, call_trans.update_date) AS duration_call,
    call_trans.voice_id
FROM 
    icx_sg.t_calllist calllist
INNER JOIN 
    t_aig_app tapp ON calllist.calllist_id  = tapp.calllist_id 
INNER JOIN 
    t_call_trans call_trans ON calllist.calllist_id = call_trans.calllist_id 
WHERE 
    calllist.calllist_id = ?;

`;

// Function to format the date to 'yyyy/mm/dd hh:mm:ss'
const formatDateToText = (dateStr) => {
  const date = new Date(dateStr);
  const year = date.getFullYear();
  const month = String(date.getMonth() + 1).padStart(2, '0');
  const day = String(date.getDate()).padStart(2, '0');
  const hours = String(date.getHours()).padStart(2, '0');
  const minutes = String(date.getMinutes()).padStart(2, '0');
  const seconds = String(date.getSeconds()).padStart(2, '0');

  return `${year}/${month}/${day} ${hours}:${minutes}:${seconds}`;
};

// Fetch and format data for a specific calllist_id
const fetchQuery = async (calllistId) => {
  try {
    const query = sqlQuery.replace('?', calllistId);
    const result = await queryDatabase(query);

    if (result.length > 0) {
      const object = Object.values(result[0]);

      // Format specific date-time fields
      object[37] = formatDateToText(object[37]);
      object[38] = formatDateToText(object[38]);
      object[43] = formatDateToText(object[43]);
      object[44] = formatDateToText(object[44]);

      const formattedLine = object.join('|') + '\n';
      return formattedLine;
    }
  } catch (error) {
    console.error(`Error fetching data for calllist_id ${calllistId}:`, error);
  }
};
const fetchQueryResponseCall = async (calllistId) => {
  try {
    const query = sqlQueryResponseCall.replace('?', calllistId);
    const result = await queryDatabase(query);

    if (result.length > 0) {
      const object = Object.values(result[0]);
      const formattedLine = object.join('|') + '\n';
      return formattedLine;
    }
  } catch (error) {
    console.error(`Error fetching data for calllist_id ${calllistId}:`, error);
  }
};

// Fetch call list IDs and write data to a file
const fetchCallListId = async () => {
  const sqlCheckCallListId = `SELECT calllist_id FROM icx_sg.t_calllist;`;

  try {
    const callIdList = await queryDatabase(sqlCheckCallListId);
    const callIds = callIdList.map((callId) => callId.calllist_id);

    const filePath = path.join(__dirname, `RESPONSE_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}.dat`);
    const filePathCall = path.join(__dirname, `RESPONSE_CALL_${new Date().toISOString().slice(0, 10).replace(/-/g, '')}.dat`);
    const fileStream = fs.createWriteStream(filePath);
    const fileStreamCall = fs.createWriteStream(filePathCall);

    for (const callId of callIds) {
      console.log("callId", callId)
      const formattedLine = await fetchQuery(callId);
      const formattedLineCall = await fetchQueryResponseCall(callId);

      if (formattedLine) {
        fileStream.write(formattedLine);
      }
      if (formattedLineCall) {
        fileStreamCall.write(formattedLineCall);
      }
    }

    fileStream.end(() => console.log(`Data successfully written to ${filePath}`));
  } catch (error) {
    console.error("Error fetching call list IDs:", error);
  }
};

fetchCallListId();
