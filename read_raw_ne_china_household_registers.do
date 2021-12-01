set more off
label drop _all

local codebook_path ""
local raw_path "../Stata raw"

* Path to the folder containing the .xls files are for the individual registers
local xls_path "C:/Users/camcam/SkyDrive/CMGPD Data"

* Read the codebooks in and produce .dta files from them
do hukouce_codebooks

if ("`codebook_path'" ~= "") {
	copy Hukouce.dta "`codebook_path'/Hukouce.dta", replace
	copy Variables.dta "`codebook_path'/Variables.dta", replace
	copy Registers.dta "`codebook_path'/Registers.dta", replace
	copy "Village master.dta" "`codebook_path'/Village master.dta", replace
	copy "Rank master.dta" "`codebook_path'/Rank master.dta", replace
	copy "Rank recodes.dta" "`codebook_path'/Rank recodes.dta", replace
	copy "Occupation master.dta" "`codebook_path'/Occupation master.dta", replace
	copy "Occupation recodes.dta" "`codebook_path'/Occupation recodes.dta", replace
	copy "Disability master.dta" "`codebook_path'/Disability master.dta", replace
	copy "Disability recodes.dta" "`codebook_path'/Disability recodes.dta", replace
}

local date_time "$S_DATE $S_TIME"
local date_time = subinstr("`date_time'",":","",.)

log using "../logs/Read registers `date_time'", text replace

display "Reading registers"

display "$S_DATE $S_TIME"

use Variables.dta

local i = 1
while (`i' <= _N) {
	local variable_name_`i' = name[`i']
	local variable_label_`i' = variable_label[`i']
	local variable_note_`i' = variable_note[`i']
	local variable_string_`i' = is_string[`i']
	local i = `i' + 1
}
local variable_count = `i' - 1

save Variables, replace

if ("`codebook_path'" ~= "") {
	save "`codebook_path'/Variables.dta", replace
}

drop _all

use "Registers" if include
*keep if dataset <= 101

replace file = substr(file,1,index(file,".")-1) if index(file,".") > 0

local i = 1
*generate file = ""
*generate sheet = ""
while (`i' <= _N) {
	local dataset`i' = string(dataset[`i'],"%03.0f")
	local date`i' = date[`i']
	local prefix`i' = prefix[`i']
	local project`i' = project[`i']
	if `project`i'' == 1 {
		local file`i' = "LN\Converted\"+file[`i']
	}
	if `project`i'' == 2 | `project`i'' == 3 {
		local file`i' = "SC\JQ&TD&FD HKC\"+file[`i']
	}
	local sheet`i' = sheet[`i']
	local xls`i' = xls[`i']
	local i = `i' + 1
}

tempfile scp notes
drop _all
*set trace on

/*
display "Verifying accessibility of files"
local j = 1
set debug on
while (`j' < `i') {
	display "`j' `file `j'' `sheet`j''"
	if (`xls`j'' == 1) {
		local codebook "Excel Files;DBQ=`xls_path'/`file`j''"
		quietly odbc load, dsn("`codebook'") table("`sheet`j''") lowercase
		drop _all
	}
	else {
		quietly insheet using "../`xls_path'/`file`j''"
		drop _all
	}
	local j = `j' + 1
}
set debug off
*/

local j = 1
while (`j' < `i') {
	display "Processing `dataset`j'' `date`j'' `file`j'' `xls`j''"
	* Look for an .xls file first
	capture import excel "`xls_path'/`file`j''.xls", case(lower) firstrow sheet("`sheet`j''") clear
	if _rc {
	* If can't find .xls file, look for an .xlsx file 
		import excel "`xls_path'/`file`j''.xlsx", case(lower) firstrow sheet("`sheet`j''") clear
	}
	local k = 1
	while(`k' <= `variable_count') {
		if(`variable_string_`k'' == 1 ) {
			quietly capture tostring `variable_name_`k'', replace
			quietly capture replace `variable_name_`k'' = "" if `variable_name_`k''== "."
			quietly capture replace `variable_name_`k'' = trim(`variable_name_`k'')
		}
		local k = `k' + 1
	}
	
*	display "Discarding known defunct variables..."
	capture drop modage 
	capture drop s4
	capture drop s5
	capture drop linkid 
	capture drop recno
	
	capture confirm variable newpage
	if !_rc {
		capture confirm variable npage
		if !_rc {
			display "Renaming newpage to npage"
			replace npage = newpage if (npage == 0 | npage == .) & (newpage != 0 & newpage != .)
			display "Newpage renamed"
			drop newpage
		}
	}

*	display "Renaming variables..."
	capture rename occupation occu
	capture rename inventory dataset
	capture rename foccupation focc
	capture rename hoccupation hocc
	capture rename ffming gfming
	capture rename gf_ming gfming
	capture rename ff_ming gfming
	capture rename f_ming fming
	capture rename ffoccupation gfocc
	capture rename h_ming hming
	capture rename h_name hname
	capture rename generation gen
	capture rename birthyear birthyr
	capture rename shuoming taolun
	capture rename sangfunianling sangfu_year
	capture rename taowang_nian taowang_year
	capture rename newpage npage

*	display "Generating identification variables..."
	generate int dataset_ck = `dataset`j''
	generate int date_ck = `date`j''
	generate int line_ck = _n
	generate byte project = `project`j''

*	display "Checking for errors..."
	foreach x of varlist * {
		local k = 1
		while("`x'" != "`variable_name_`k''" & `k' <= `variable_count') {
			local k = `k' + 1
		}
*		display "`k' x:`x' variable_name_k:`variable_name_`k'' `variable_string_`k''"
		if(`k' > `variable_count') {
			display "`dataset`j'' `date`j'' `file`j'' `xls`j''"
			display "ERROR: Unrecognized variable `x'"
				local y: type `x'
				if (substr("`y'",1,3) == "str") {	 
					gsort -`x'
					list case `x' if _n < 6
				} 
				else {
					quietly replace `x' = 0 if `x' == .
					gsort -`x'
					list case `x' if _n < 6
				}	
			quietly drop `x'
		}
		else {
			local y: type `x'
*			If a variable is supposed to be numeric, but has been imported as a string, force it to numeric, print out an error message, and proceed...	
			if ((`variable_string_`k'' == 0) & (substr("`y'",1,3) == "str")) {
				quietly generate invalid = real(`x') == . & `x' != ""
				quietly: summarize invalid 	
				if (r(max)) { 
					display "`dataset`j'' `date`j'' `file`j'' `xls`j''"
					display "ERROR: Numeric variable `x' contains non-numeric characters" 
					list case date interpol `x' if real(`x') == . & `x' != ""
				}
				quietly replace `x' = substr(`x',1,indexnot(`x',"0123456789")-1) if indexnot(`x',"0123456789") > 0
				quietly destring `x', ignore("() ") replace force
				quietly replace `x' = 0 if `x' == .
				quietly drop invalid
			}
* 			If a variable is supposed to be string, but has been imported as a numeric, force it to string, and print out a warning...
			if ((`variable_string_`k'' == 1) & (substr("`y'",1,3) != "str")) {
				tostring `x', replace force
				replace `x' = "" if `x' == "."
				display "WARNING: String variable `x' was imported as a numeric variable by STATA, and was converted to a string variable."
			}
		}
	}

	quietly keep if case != . & case != 0

*	foreach x in fname gfname jingguan hname guanling zhuangtou dengji hxing hming fxing fming gfming sming taolun {
*		quietly capture drop `x'
*	}

/*	replace dataset_ck = 25 if dataset == 29
	replace case = case+10000 if dataset == 29
	replace line_ck = line_ck+10000 if dataset == 29
	replace clan = clan+1000 if dataset == 29
	replace hid = hid+1000 if dataset == 29
	replace lcase = lcase+10000 if dataset == 29 & date > 861 & lcase != 0 & lcase != .
*	replace ldataset = 25 if dataset == 29
	replace dataset = 25 if dataset == 29
*/

* Note and taolun are too large to keep, and Stata can't handle Chinese anyway, so create has_note to indicate whether an observation had a note or taolun, then drop note and taolun.  

	generate byte has_note = 0
	capture confirm variable taolun
	if (_rc == 0) {
		quietly replace has_note = taolun != ""
		drop taolun
	}

	capture confirm variable note
	if (_rc == 0) {
		quietly replace has_note = 1 if note != ""
		drop note
	}

	capture confirm variable new_page
	if(_rc == 0) {
		capture drop npage
		rename new_page npage
	}

* Create the ldataset variable
	quietly	replace dataset = dataset_ck

	capture confirm variable ldataset
	if(_rc != 0) {
		generate ldataset = 0
	}
	quietly replace ldataset = dataset if (ldataset == 0 | ldataset == .) & ldate != 0 & ldate != .

	capture confirm variable linterpol
	if(_rc != 0) {
		generate linterpol = 0
	}
	quietly replace linterpol = 0 if linterpol == .	

/*

	replace lcase = 0 if dataset == 102 & ldataset == 101

	replace linterpol = 0 if dataset == 102 & ldataset == 101
	replace ldate = 0 if dataset == 102 & ldataset == 101
	replace ldataset = 0 if dataset == 102 & ldataset == 101
*/

	quietly replace date = date_ck
	sort dataset_ck line_ck
	compress
	quietly save "`raw_path'/raw`j'", replace	
	quietly drop _all
	local j = `j' + 1
}

log close
