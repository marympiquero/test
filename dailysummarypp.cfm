<cfscript>	
	var datenow = DateFormat(Now(),'yyyy-mm-dd');
	var startdate = DateFormat(DateAdd("d",-1,Now()), 'yyyy-mm-dd');	
	var logfile = new lib.logfile("/log/dailysummary_job_#datenow#.log");
	include("/lib/user.cfc");
	
	var datenow = '2016-05-12';
	var startdate = '2016-05-11';
</cfscript>

<cfscript>
	writeoutput("Starting daily summary job..");
	logfile.writeline("Starting daily summary job..");
</cfscript>

<!--- Retrieve All Merchants --->
<cftry>
	<cfscript>
		writeoutput("Retrieving active merchants..");
		logfile.writeline("Retrieving active merchants..");
	</cfscript>
	
	<cfquery name="merchants">
		SELECT merchantid, contactname, email, verticalid 
		FROM merchants 
		WHERE accountactive = <cfqueryparam value="1" cfsqltype="cf_sql_smallint">
		AND testmode = <cfqueryparam value="0" cfsqltype="cf_sql_smallint">
		AND deleted = <cfqueryparam value="0" cfsqltype="cf_sql_smallint">
		<cfif structkeyexists(url, 'limit') AND isnumeric(url.limit)>
			LIMIT <cfqueryparam value="#url.limit#" cfsqltype="cf_sql_integer">
		</cfif>
	</cfquery>
	
	<cfscript>
		writeoutput("Retrieved #merchants.recordcount# merchants..");
		logfile.writeline("Retrieved #merchants.recordcount# merchants..");
	</cfscript>
	
	<cfcatch>
		<cfdump var=#cfcatch#>
		<cfscript>
			writeoutput("Error in the retrieval of active merchants: #cfcatch.message#");
			logfile.writeline("Error in the retrieval of active merchants: #cfcatch.message#");
		</cfscript>
	</cfcatch>
</cftry>

<!--- Loop Though All Merchants and retrieve daily summary for each--->
<cftry>
	<cfloop query="merchants">
		<!--- Total Payments --->
		<cftry>
			<cfscript>
				writeoutput("Retrieving transactions for merchant: #merchants.merchantid#..");
				logfile.writeline("Retrieving transactions for merchant: #merchants.merchantid#..");
				writeoutput("Retrieving payment transactions..");
				logfile.writeline("Retrieving payment transactions..");
			</cfscript>
			
			<cfquery name="totals">
				SELECT merchantid, COUNT(*) AS count, SUM(amount) AS volume 
				FROM payments
				WHERE voided != 1
				AND
				(
					(paymenttype = 1)
					OR
					(paymenttype = 2 AND authcode IS NOT NULL AND authcode <> '' AND voided = 0)
				)
				AND paymentdate BETWEEN <cfqueryparam value="#startdate#" cfsqltype="cf_sql_date"> AND <cfqueryparam value="#datenow#" cfsqltype="cf_sql_date">
				AND merchantid = <cfqueryparam value="#merchants.merchantid#" cfsqltype="cf_sql_integer">
				GROUP BY merchantid
			</cfquery>
			
			<cfcatch>
				<cfdump var=#cfcatch#>
				<cfscript>
					writeoutput("Error retrieving payment transactions: #cfcatch.message#..");
					logfile.writeline("Error retrieving payment transactions: #cfcatch.message#..");
				</cfscript>
			</cfcatch>
		</cftry>

		<!--- Total CC Transactions --->
		<!--- Total CC Volume --->
		<cftry>
			<cfscript>
				writeoutput("Retrieving credit card transactions..");
				logfile.writeline("Retrieving credit card transactions..");
			</cfscript>
			
			<cfquery name="card_totals">
				SELECT merchantid, COUNT(*) AS count, SUM(amount) AS volume 
				FROM payments
				WHERE (paymenttype = 2 AND authcode IS NOT NULL AND authcode <> '' AND voided = 0)
				AND paymentdate BETWEEN <cfqueryparam value="#startdate#" cfsqltype="cf_sql_date"> AND <cfqueryparam value="#datenow#" cfsqltype="cf_sql_date">
				AND amount > 0
				AND voided != 1
				AND merchantid = <cfqueryparam value="#merchants.merchantid#" cfsqltype="cf_sql_integer">
				GROUP BY merchantid
			</cfquery>
			
			<cfcatch>
				<cfdump var=#cfcatch#>
				<cfscript>
					writeoutput("Error in retrieving credit card transactions: #cfcatch.message#..");
					logfile.writeline("Error in retrieving credit card transactions: #cfcatch.message#..");
				</cfscript>
			</cfcatch>
		</cftry>	


		<!--- Total ACH Transactions --->
		<!--- Total ACH Volume --->
		<cftry>
			<cfscript>
				writeoutput("Retrieving ACH transactions..");
				logfile.writeline("Retrieving ACH transactions..");
			</cfscript>
			
			<cfquery name="ach_totals">
				SELECT merchantid, COUNT(*) AS count, SUM(amount) AS volume 
				FROM payments
				WHERE paymenttype = 1
				AND paymentdate BETWEEN <cfqueryparam value="#startdate#" cfsqltype="cf_sql_date"> AND <cfqueryparam value="#datenow#" cfsqltype="cf_sql_date">
				AND amount > 0
				AND voided != 1
				AND merchantid = <cfqueryparam value="#merchants.merchantid#" cfsqltype="cf_sql_integer">
				GROUP BY merchantid
				ORDER BY merchantid
			</cfquery>
			
			<cfcatch>
				<cfdump var=#cfcatch#>
				<cfscript>
					writeoutput("Error in retrieving ACH transactions: #cfcatch.message#..");
					logfile.writeline("Error in retrieving ACH transactions: #cfcatch.message#..");
				</cfscript>
			</cfcatch>
		</cftry>

		<!--- Total Refunds Issued --->
		<cftry>
			<cfscript>
				writeoutput("Retrieving refund transactions..");
				logfile.writeline("Retrieving refund transactions..");
			</cfscript>
			
			<cfquery name="refunds">
				SELECT merchantid, COUNT(*) AS count, SUM(amount) AS volume 
				FROM refunds 
				WHERE refunddate BETWEEN <cfqueryparam value="#startdate#" cfsqltype="cf_sql_date"> AND <cfqueryparam value="#datenow#" cfsqltype="cf_sql_date">
				AND status = <cfqueryparam value="5" cfsqltype="cf_sql_integer">
				AND merchantid = <cfqueryparam value="#merchants.merchantid#" cfsqltype="cf_sql_integer">
				GROUP BY merchantid
			</cfquery>
			
			<cfcatch>
				<cfdump var=#cfcatch#>
				<cfscript>
					writeoutput("Error in retrieving refund transactions: #cfcatch.message#..");
					logfile.writeline("Error in retrieving refund transactions: #cfcatch.message#..");
				</cfscript>
			</cfcatch>
		</cftry>


		<!--- Total Payment Alerts --->
		<cftry>
			<cfscript>
				writeoutput("Retrieving payment alerts..");
				logfile.writeline("Retrieving payment alerts..");
			</cfscript>
			
			<cfquery name="alerts">
				SELECT merchantid, COUNT(*) as count 
				FROM alertstatus 
				WHERE entrydate BETWEEN <cfqueryparam value="#startdate#" cfsqltype="cf_sql_date"> AND <cfqueryparam value="#datenow#" cfsqltype="cf_sql_date">
				AND ACTIVE = true
				AND merchantid = <cfqueryparam value="#merchants.merchantid#" cfsqltype="cf_sql_integer">
				GROUP BY merchantid
			</cfquery>
			
			<cfcatch>
				<cfdump var=#cfcatch#>
				<cfscript>
					writeoutput("Error in retrieving payment alerts: #cfcatch.message#..");
					logfile.writeline("Error in retrieving payment alerts: #cfcatch.message#..");
				</cfscript>
			</cfcatch>
		</cftry>

		<!--- Total ACH Returns --->
		<cftry>
			<cfscript>
				writeoutput("Retrieving ACH returns..");
				logfile.writeline("Retrieving ACH returns..");
			</cfscript>
			
			<cfquery name="ach_returns">	
				SELECT merchantid, COUNT(*) AS count, SUM(amount) as volume 
				FROM achitems
				WHERE entrydate BETWEEN <cfqueryparam value="#startdate#" cfsqltype="cf_sql_date"> AND <cfqueryparam value="#datenow#" cfsqltype="cf_sql_date">
				AND merchantid = <cfqueryparam value="#merchants.merchantid#" cfsqltype="cf_sql_integer">
				GROUP BY merchantid
			</cfquery>
			
			<cfcatch>
				<cfdump var=#cfcatch#>
				<cfscript>
					writeoutput("Error in retrieving ACH returns: #cfcatch.message#..");
					logfile.writeline("Error in retrieving ACH returns: #cfcatch.message#..");
				</cfscript>
			</cfcatch>
		</cftry>	
		
		<!--- construct report in the body of the email--->
		<cftry>
			<cfscript>
				writeoutput("Generating report...");
				logfile.writeline("Generating report...");
			</cfscript>
			
			<cfset transaction_labels = ['All Payments', 'Credit Card Payments', 'ACH Payments', 'All Refunds', 'Payment Alerts']>
			<cfset transaction_types = ['totals', 'card_totals', 'ach_totals', 'refunds', 'alerts_ach_returns']>
			
			<cfsavecontent variable="summary">
				<table border="1" cellpadding="5" cellspacing="1" width="70%">
					<tr>
						<th>   </th>
						<th>No. of Transactions</th>
						<th>Volume</th>
					</tr>
					
					<!---Combine ach_returns and alerts--->
					<cfset alerts_ach_returns = {}>
					<cfset alerts_ach_returns.count = 0>
					<cfset alerts_ach_returns.volume = 0>
					<cfset alerts_ach_returns.count = alerts_ach_returns.count + NumberFormat(alerts.count) + NumberFormat(ach_returns.count)>
					<cfset alerts_ach_returns.volume = alerts_ach_returns.volume + NumberFormat(ach_returns.volume)>
					
					<cfloop from=1 to=#arraylen(transaction_types)# index="type">
						<cfset type_count = transaction_types[type] & '.count'>
						<cfset type_volume = transaction_types[type] & '.volume'>
						
						<tr>
							<td><cfoutput>#transaction_labels[type]#</cfoutput></td>
							<td><cfoutput>#NumberFormat(Evaluate(type_count))#</cfoutput></td>
							<td><cfoutput>#DollarFormat(Evaluate(type_volume))#</cfoutput></td>
						</tr>
					</cfloop>
				</table>
			</cfsavecontent>
			
			<cfscript>
				writeoutput("Report generated...");
				logfile.writeline("Report generated...");
			</cfscript>
			
			<cfcatch>
				<cfdump var=#cfcatch#>
				<cfscript>
					writeoutput("Error occured while generating the report: #cfcatch.message#");
					logfile.writeline("Error occured while generating the report: #cfcatch.message#");
				</cfscript>
			</cfcatch>
		</cftry>	

		<!---create spreadsheet for attachment--->
		<!---
		<cftry>
			<cfset reportFile = "/marytest/dailysummary_#datenow#_#merchants.merchantid#.csv">

			<cfset reportSheet = SpreadsheetNew("Daily Summary")>
			<cfset SpreadsheetAddRow(reportSheet, " - , Transactions, Volume")>
			<cfset SpreadsheetAddRow(reportSheet, "Payments, #NumberFormat(totals.count)#, #DollarFormat(totals.volume)#")>
			<cfset SpreadsheetAddRow(reportSheet, "Credit Card, #NumberFormat(card_totals.count)#, #DollarFormat(card_totals.volume)#")>
			<cfset SpreadsheetAddRow(reportSheet, "ACH, #NumberFormat(ach_totals.count)#, #DollarFormat(ach_totals.volume)#")>
			<cfset SpreadsheetAddRow(reportSheet, "Refunds, #NumberFormat(refunds.count)#, #DollarFormat(refunds.volume)#")>
			<cfset SpreadsheetAddRow(reportSheet, "Payment Alerts, #NumberFormat(alerts.count)#, --")>
			<cfset SpreadsheetAddRow(reportSheet, "ACH Returns, #NumberFormat(ach_returns.count)#, #DollarFormat(ach_returns.volume)#")>

			<cfdump var=#reportSheet#>

			<cfspreadsheet action="write" filename="#reportFile#" name="#reportSheet#" overwrite=true>
			
			<cfcatch>
				<cfdump var=#cfcatch#>
				<cfscript>
					writeoutput("Error in creating the spreadsheet file: #cfcatch.message#");
					logfile.writeline("Error in creating the spreadsheet file: #cfcatch.message#");
				</cfscript>
			</cfcatch>
		</cftry>	
		--->
		
		<cftry>
			<cfif structkeyexists(url, 'email') and len(url.email)><!--- send to the email provided in the url, for testing purposes--->
				<cfset emailto = url.email> 
			<cfelse>	
				<cfset emailto = merchants.email>
			</cfif>
			
			<cfset sender = 'Convenient Payments'>
			
			<!---get the name of the tellers--->
			<!---- teller names without having this verticalids 14, 15, 18, 19, 9, 22 can be derived from the logo name, we will just remove the word logo.png ---->
			<cfset teller = ''>
			<cfif merchants.verticalid EQ 14>
				<cfset teller = 'Business Payment Advisors'>
			<cfelseif merchants.verticalid EQ 15>
				<cfset teller = 'Omega'>
			<cfelseif merchants.verticalid EQ 18>
				<cfset teller = 'Dental Card Services'>
			<cfelseif merchants.verticalid EQ 19>
				<cfset teller = 'GolfTranz Golf Industry Pmts'>
			<cfelseif merchants.verticalid EQ 9>	
				<cfset teller = 'USMS Merchants'>
			<cfelseif merchants.verticalid EQ 22>
				<cfset teller = 'Orion Payment Systems'>
			<cfelse>
				<cfset teller = ReplaceNoCase(get_cp_logo(merchants.merchantid), 'logo.png', '')>
			</cfif>
			
			<!---content of the message--->
			<cfsavecontent variable="message">
				Hi <cfoutput>#merchants.contactname#</cfoutput>,
				<br><br>
				Here is the summary for the day:
				<br><br>
				<cfoutput>#summary#</cfoutput>
				<br><br>
				
				
				Thank you,
				<br>
				<img src='/img/logos/<cfoutput>#get_cp_logo(merchants.merchantid)#</cfoutput>' alt="<cfoutput>#teller#</cfoutput>">
				<br>Customer Support: 855-872-6632
				<br>Email: support@convenientpayments.com
			</cfsavecontent>
			
			<cfoutput> #message# </cfoutput>
		
			<cfscript>
				writeoutput("Sending the email report to #emailto#(Merchant ID: #merchants.merchantid#)...");
				logfile.writeline("Sending the email report to #emailto#(Merchant ID: #merchants.merchantid#)...");
			
				var email = new lib.email();
				email.send(emailto,"Daily Summary", message, message, "noreply@convenientpayments.com", merchants.contactname, "#sender#", "joshua.zirbel@convenientpayments.com", "");
			
				writeoutput("Email sent to #emailto#(Merchant ID: #merchants.merchantid#)...");
				logfile.writeline("Email sent to #emailto#(Merchant ID: #merchants.merchantid#)...");
			</cfscript>
			
			<cfcatch>
				<cfdump var=#cfcatch#>
				<cfscript>
					writeoutput("Error in sending the email to #emailto#(Merchant ID: #merchants.merchantid#): #cfcatch.message#...");
					logfile.writeline("Error in sending the email to #emailto#(Merchant ID: #merchants.merchantid#): #cfcatch.message#...");
				</cfscript>
			</cfcatch>
		</cftry>
	</cfloop>	
	
	<cfscript>
		writeoutput("All daily summary have been sent to #merchants.recordcount# merchants...");
		writeoutput("Job completed...");
		logfile.writeline("All daily summary have been sent to #merchants.recordcount# merchants...");
		logfile.writeline("Job completed...");
	</cfscript>
	
	<cfcatch>
		<cfdump var=#cfcatch#>
		<cfscript>
			writeoutput("Error in processing daily summary job: #cfcatch.message#..");
			logfile.writeline("Error in processing daily summary job: #cfcatch.message#..");
		</cfscript>
	</cfcatch>
</cftry>
