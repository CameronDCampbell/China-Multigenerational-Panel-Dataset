set more off

drop _all
program drop _all
capture log close

program define save_generated
display "Saving household registers generated"
desc, short
save "../combined/household_registers_generated", replace
end

program define mk_consolidated_file

display "mk_consolidated_file: Reading everything back in"

local raw_dir "../Stata raw/"

local j = 1
capture use "`raw_dir'raw`j'"
local end = _rc

while (`end' == 0) {
	capture replace name = trim(name)
	capture replace rhhead = trim(rhhead)
	capture replace xing = trim(xing)
	capture replace ming = trim(ming)
	compress
*	capture drop taolun
*	capture drop note
	save, replace
	local j = `j' + 1
	capture use "`raw_dir'raw`j'"
	local end = _rc
}

local j = 2

use "`raw_dir'raw1"

local end = 0
while (`end' == 0) {
	capture append using "`raw_dir'raw`j'"
	display `j' _rc
	local j = `j' + 1
	local end = _rc
}

foreach var in varlist rhhead name cam {
	capture replace `var' = "" if `var' == "."
}

replace name = trim(ming) if dataset >= 100 & ming != "" & xing == ""
replace name = trim(xing) + " " + trim(ming) if xing != "" & ming != ""
replace name = trim(xing) if xing != "" & ming == ""

*generate byte has_note = note != "" | taolun != ""

/*
foreach var in varlist rhhead name cam fname gfname jingguan hname guanling zhuangtou dengji {
	capture replace `var' = "" if `var' == "."
}

*/

quietly replace dataset = dataset_ck 
quietly replace date = date_ck

label define dataset1 1 "Daoyi" 2 "Gaizhou" 3 "Dami" 4 "Chengnei" 5 "Mianding" 6 "Niuzhuang" 7 "Feicheng" 8 "Manhan" 9 "Dadianzi" 10 "Guosantun" 11 "Bakeshu" 12 "Daxingtun" 13 "Nianma Dahai" 14 "Changzhaizi"  15 "Zhaohuatun" 16 "Diaopitun" 17 "Langjiabao" 18 "Wangzhihuitun" 19 "Aerjishan" 20 "Haizhou" 21 "WDLS Shengding" 22 "WDLS Rending" 23 "Waziyu" 24 "Wuhu" 25 "Mianyanding" 26 "Suba" 27 "Kaidang" 28 "Kaidang Toucong Baoyang" 29 "Mianhua Yanding Xiaomen Rending"

label define dataset1 101 "TD ZhHu" 102 "TD XiHu" 103 "TD ZhBa" 104 "TD XiBa" 105 "TD ZhHo" 106 "TD XiHo" 107 "TD ZhLa" 108 "TD XiLa" 111 "FD ZhBa" 112 "FD XiBa"  113 "FD ZhHo" 114 "FD XiHo" 115 "FD ZhLa" 116 "FD XiLa", modify

label values dataset dataset1

label define project1 1 "Liaoning" 2 "SCP Tunding" 3 "SCP Fuding"
label values project project1

replace name = trim(name)
replace rhhead = trim(rhhead)

desc

preserve

keep dataset dataset_ck line_ck date_ck case date interpol fname gfname jingguan hname guanling zhuangtou dengji zuoling_ming xing ming hxing hming fxing fming gfming sming has_note

order dataset case date interpol dataset_ck date_ck line_ck fname gfname jingguan hname guanling zhuangtou dengji zuoling_ming xing ming hxing hming fxing fming gfming sming has_note

generate byte alphabetic = 0
foreach x of varlist fname-sming {
	replace alphabetic = length(trim(`x')) + alphabetic
	}
drop if alphabetic == 0
drop alphabetic

save "../combined/Household register names and notes", replace

keep if has_note 

keep dataset_ck date_ck line_ck has_note

sort dataset_ck date_ck line_ck

save "../combined/Household register entries with notes", replace

restore

drop fname gfname jingguan hname guanling zhuangtou dengji xing ming hxing hming fxing fming gfming sming

outsheet using "Invalid dataset or date.txt" if dataset != dataset_ck | date != date_ck, replace 

mvencode _all, mv(0) override

tab dataset

tab dataset_ck

sort dataset_ck date_ck line_ck

if ("`1'" == "test") {
	keep if (dataset <= 2 | dataset == 25 | dataset == 29 | dataset == 101 | dataset == 102)  
}

save "../combined/household_registers_raw", replace

drop _all

display "mk_consolidated_file: exiting"

end

program define check
use ../combined/household_registers_ancestors
sort id
brow
drop _all
end

program define ancestors 
father_id "`1'"
mother_id "`1'"
father_id_2 "`1'"
father_id_3 "`1'"
father_id_4 "`1'""
father_id_4a "`1'""
father_id_6 "`1'"
father_id_4 "`1'"
father_id_4a "`1'""
founder_ancestor
founder_group_1
founder_group_2
founder_yihu
end

program define generated
household_registers_generated_1
household_registers_generated_2
household_registers_generated_3
name_variables
kin_variables
occupation_variables
fertility_variables
unique_address
fix_names
end

program define from_founder_2
founder_2
*founder_3
household_registers_generated_1
household_registers_generated_2
end

program define link_individuals
*set trace on
display "link_individuals: linking individual observations across registers"

*local restriction "if dataset == 1"

local date_time "`1'"
use "../combined/household_registers_raw" `restriction'
tempfile link
keep dataset case date interpol lcase ldate linterpol ldataset
rename lcase pcase
rename ldate pdate
rename linterpol pinterpol
rename ldataset pdataset
rename case fcase
rename date fdate
rename interpol finterpol
rename dataset fdataset
sort fdataset fcase fdate finterpol
save "`link'"

use "../combined/household_registers_raw" `restriction'

sort dataset date line

by dataset: generate long recno = _n 

generate fdataset = ldataset
generate fcase = lcase 
generate fdate = ldate
generate finterpol = linterpol
sort fdataset fcase fdate finterpol
merge fdataset fcase fdate finterpol using "`link'", nokeep
summ dataset
mvencode pdataset pcase pdate pinterpol, mv(0) override
drop _merge
generate sumpdate = sum(pdate)
local i = 0
while ((sumpdate[_N] > 0) & (`i' < 50)) {
	replace fdataset = pdataset if pdataset > 0
	replace fcase = pcase if pcase > 0
	replace fdate = pdate if pdate > 0
	replace finterpol = pinterpol if pcase > 0
	drop pdataset pcase pdate pinterpol
	sort fdataset fcase fdate finterpol
	merge fdataset fcase fdate finterpol using "`link'", nokeep
	sort dataset recno
	by dataset recno: keep if _n == 1
	local i = `i'+1
	mvencode pdataset pcase pdate pinterpol, mv(0) override
	drop _merge
	replace sumpdate = sum(pdate)
}
drop sumpdate

outsheet using "../logs/Excessive links `date_time'.txt" if pdate > 0, replace

replace fdataset = dataset if fdataset == 0
replace finterpol = interpol if fcase == 0 
replace fcase = case if fcase == 0
replace fdate = date if fdate == 0

drop pcase pdate pinterpol pdataset
sort fdataset fdate fcase finterpol date 
by fdataset fdate fcase finterpol: generate byte new_person = _n == 1
capture drop id
generate id = sum(new_person)
drop new_person fdataset fcase fdate finterpol
sort dataset date case interpol

order dataset case date interpol id lcase ldate linterpol name 

sort dataset date case interpol

desc

display "link_individuals: saving linked file"

save "../combined/household_registers_raw", replace

replace name = trim(name)
replace rhhead = trim(rhhead)

*save "../combined/household_registers_raw `date_time'", replace

generate byte version = 1
display "link_individuals: creating change file"

/*

append using "../combined/household_registers_raw_backup"

if ("`restriction'" ~= "") {
	keep `restriction'
}

if ("`2'" == "test") {
	keep if (dataset <= 2 | dataset == 25 | dataset == 29 | dataset == 101 | dataset == 102)
}

replace version = 2 if version == .

keep case date interpol clan hid yihu rhhead address sex gen rank occu marital name vital1 vital2 birthyr age month day hour disabil lcase ldate linterpol id dataset focc gfocc jgocc hocc version

order case date interpol clan hid yihu rhhead address sex gen rank occu marital name vital1 vital2 birthyr age month day hour disabil lcase ldate linterpol id dataset focc gfocc jgocc hocc version

sort dataset date case interpol version
generate byte change = 0

by dataset date case interpol: replace change = 1 if _N == 1 & version == 1
by dataset date case interpol: replace change = 2 if _N == 1 & version == 2

generate str30 changes = ""

foreach var of varlist * {
	if ("`var'" != "change" & "`var'" != changes) {
		by dataset date case interpol: replace change = 3 if _N == 2 & `var'[1] != `var'[2]
	}
}

keep if change != 0

foreach var of varlist * {
	if ("`var'" != "change" & "`var'" != changes) {		
		by dataset date case interpol: replace changes = changes + "`var' " if `var'[1] != `var'[2]
	}
}

keep if change != 0

*save "../combined/household_registers_raw changes `date_time'", replace

*/

drop _all

use "../combined/household_registers_raw"
save "../combined/household_registers_raw_backup", replace
drop _all
*set trace off
display "link_individuals: exiting"
end

program define write_raw
display "write_raw: entering"
use "Liaoning registers"
sort dataset date
local i = 1
local j = _N
while (`i' <= `j') {
	local dataset`i' = string(dataset[`i'],"%02.0f")
	local date`i' = date[`i']
	local prefix`i' = Prefix[`i']
	local directory`i' = Directory[`i']
	local i = `i'+1
}
drop _all
use ../combined/household_registers_raw
order case date interpol clan hid yihu rhhead address modage s5 sex gen rank occu marital name vital1 vital2 birthyr age month day hour disabil cam s4 lcase ldate linterpol id dataset fname focc gfname gfocc jingguan jgocc hname hocc juzhudi guanling zhuangtou dengji
keep case-dengji
local i = 1
display "`i' `j'"
while (`i' <= `j') {
	display "`i' `dataset`i'' `date`i''"
	outsheet case-dengji if dataset == `dataset`i'' & date == `date`i'' using "../Raw/`dataset`i''_`prefix`i''_`date`i''.txt", nolabel replace
	local i = `i' + 1
}
drop _all
display "write_raw: exiting"
end

program define error_check
display "error_check: entering"
local date_time = "`1'"
use ../combined/household_registers_raw
sort dataset date case interpol
*by dataset: generate recno = _n
generate str16 error = ""
display "error_check: check for 01: Invalid sex"
replace error = "001 " + error if sex != 1 & sex != 2
display "error_check: check for 02: Blank relationship"
replace error = "002 " + error if rhhead == ""
display "error_check: check for 03: Sex = 2 but rh ends in c or w"
replace error = "003 " + error if sex == 2 & (substr(rtrim(rhhead),-1,1) == "w" | substr(rtrim(rhhead),-1,1) == "c")
replace error = "004 " + error if sex == 2 & substr(rtrim(rhhead),-1,1) == "m"
replace error = "005 " + error if sex == 2 & substr(rtrim(rhhead),-1,1) == "d"

display "error_check: check for 06: Sex = 1, marital != 2, vital1 != 2, but rh does not end in w, c or m"
replace error = "006 " if sex == 1 & (vital1 == 0 | vital1 >= 6) & (marital != 2 & vital1 != 2) & substr(rtrim(rhhead),-1,1) != "c" & substr(rtrim(rhhead),-1,1) != "w" & & substr(rtrim(rhhead),-1,1) != "m" & trim(rhhead) != "e"

display "error_check: check for 07: Never-married woman not coded as a daughter or sister"
replace error = "007 " if sex == 1 & marital == 2 & substr(rtrim(rhhead),-1,1) != "d" & substr(rtrim(rhhead),-1,1) != "z" & rtrim(rhhead) != "e"

display "error_check: check for 08: repeated case, date, interpol"
sort dataset date case interpol
by dataset date case interpol: replace error = "008 " + error if _N > 1
sort dataset ldate lcase linterpol

display "error_check: check for 09: repeated lcase, ldate, linterpol"
by dataset ldate lcase linterpol: replace error = "009 " + error if _N > 1 & vital1 != 8 & vital2 != 8 & (lcase != 0 & ldate != 0 & linterpol != 0)
sort id date

by id: replace error = "010 " + error if _n == 1 & (lcase != 0 | ldate != 0 | linterpol != 0)

replace error = "011 " + error if ldate >= date

sort dataset date case interpol
by dataset date: replace error = "012 " + error if _n > 1 & trim(rhhead) != "e" & clan != clan[_n-1] 
by dataset date: replace error = "013 " + error if _n > 1 & trim(rhhead) != "e" & hid != hid[_n-1]
by dataset date: replace error = "014 " + error if _n > 1 & trim(rhhead) == "e" & (clan == clan[_n-1] & hid == hid[_n-1]) 
by dataset date: replace error = "015 " + error if _n > 1 & clan == clan[_n-1] & hid < hid[_n-1]
by dataset date: replace error = "016 " + error if _n > 1 & clan < clan[_n-1]

sort dataset date clan hid rhhead
by dataset date clan hid rhhead: replace error = "027 " + error if _N > 1 & substr(trim(rhhead),-1,1) != "w"

replace error = "028 " + error if indexnot(trim(rhhead),"0123456789efmbzsdwoyq") > 0

sort dataset_ck date_ck recno

display "error_check: checking for 029: decrease in case, interpol"

by dataset_ck date_ck:replace error = "029 " + error if _n > 1 & case*100+interpol < case[_n-1]*100+interpol[_n-1]

display "error_check: checking for 030: inconsistent value for dataset"
replace error = "030 " + error if dataset != dataset_ck

display "error_check: checking for 031: inconsistent value for date"
replace error = "031 " + error if date != date_ck

keep if error != ""

write_error_file "../logs/Error report `date_time'" ""
drop _all
display "error_check:exiting"
end

program define write_error_file
display "write_error_file: entering"

tab dataset

tempvar not_empty any_not_empty recno

local i = 1
generate `not_empty' = strlen(error) > 0
generate `any_not_empty' = sum(`not_empty')
local done_flag = `any_not_empty'[_N]
while (`done_flag' > 0 & `i' <= 5) {
	generate error_`i' = substr(error,1,3) if strlen(error) > 0
	replace error = substr(error,5,.) if strlen(error) > 0
	replace `not_empty' = strlen(error) > 0
	replace `any_not_empty' = sum(`not_empty')
	local done_flag = `any_not_empty'[_N]
	local i = `i' + 1
}

drop error
sort dataset recno
generate `recno' = _n
sort `recno'
reshape long error_, i(`recno')
rename error_ error
drop _j
drop if trim(error) == ""

if ("`2'" == "append") {
	capture confirm file "`1'.dta"
	display "Return code " _rc
	if (_rc == 0) {
		append using "`1'"
	} 
	else {
		display "New file"
	}	
}

keep case-recno error

sort dataset recno error
save "`1'", replace
drop _all
display "write_error_file: exiting"
end

program define fix_names
use ../combined/household_registers_ancestors

capture drop unique_group
capture drop unique_yihu
generate long unique_group = founder_group_dataset*1000000+founder_group_date*1000+founder_group_yihu
generate long unique_yihu = founder_yihu_dataset*1000000+founder_yihu_date*1000+founder_yihu

save, replace

use ../combined/household_registers_generated
capture drop unique_group
capture drop unique_yihu
generate long unique_group = founder_group_dataset*1000000+founder_group_date*1000+founder_group_yihu
generate long unique_yihu = founder_group_dataset*1000000+founder_yihu_date*1000+founder_yihu

save, replace
drop _all

end

program define marriages
display "marriages: entering"
*set trace on
*set tracedepth 1

local date_time = "`1'"

tempvar new_marriage
tempfile husbands savefile

use dataset date clan hid id case interpol name rhhead sex vital1 using "../combined/household_registers_raw" if sex == 2
drop sex
rename rhhead rh_husband
sort dataset date clan hid rh_husband
rename case hcase
rename interpol hinterpol
rename name hname
rename id h_id
rename vital1 hvital1 

save "`husbands'"

use ../combined/household_registers_raw if sex == 1 & (vital1 == 0 | vital1 >= 6) & (substr(trim(rhhead),-1,1) == "m" | substr(trim(rhhead),-1,1) == "w" | substr(trim(rhhead),-1,1) == "c")

sort dataset date clan hid case interpol

mk_h_rhhead rhhead rh_husband
sort dataset date clan hid rh_husband
merge dataset date clan hid rh_husband using "`husbands'", nokeep
drop _merge

display "marriages: checking for 017 ERROR: Marital != 2, 3, vital1 != 2, but no husband located"
generate str16 error = "017 " if (marital !=2 & marital != 3) & (vital1 == 0 | vital1 >= 6) & hcase == .
replace error = "018 " + error if (marital == 2) & (vital1 == 0 | vital1 >= 6) & hcase != .
replace error = "019 " + error if (marital == 3) & (vital1 == 0 | vital1 >= 6) & hcase != . & hvital1 != 1

sort dataset recno
by dataset recno: replace error = "020 " + error if _N > 1
sort id date
by id: replace error = "021 " + error if _n > 1 & h_id != h_id[_n-1] & h_id[_n-1] != . & h_id != .
sort dataset recno

save "`savefile'"

write_error_file "../logs/Error report `date_time'" append

use "`savefile'"
replace error = ""

keep if (vital1 == 0 | vital1 >= 6) & h_id != .

sort h_id date id

by h_id date: replace error = "022 " if _N > 1

*brow if error == "022 "

write_error_file "../logs/Error report `date_time'" append

use "`savefile'"
replace error = ""

rename id w_id

rename vital1 wvital1
rename name wname
rename case wcase
rename interpol winterpol

keep if h_id != .
keep dataset date h_id w_id hvital1 wvital1 wcase winterpol hcase hinterpol

sort h_id w_id date

by h_id w_id: generate byte `new_marriage' = _n == 1
generate int marriage_id = sum(`new_marriage')
by h_id w_id: generate int marriage_first = date[1]
by h_id w_id: generate int marriage_last = date[_N]

sort h_id marriage_first w_id

drop `new_marriage'

sort h_id date

save "../combined/household_registers_marriages", replace

use "../combined/household_registers_raw" if sex == 2

rename id h_id

sort h_id date
merge h_id date using "../combined/household_registers_marriages", nokeep
drop _merge

generate error = "023 " if marital != 2 & marital != 3 & (w_id == .) & (vital1 == 0 | vital1 >= 6)
replace error = "024 " + error if marital == 2 & w_id != . & (vital1 == 0 | vital1 >= 6)
replace error = "025 " + error if marital == 3 & w_id != . & (wvital1 == 0 | wvital1 >= 6) & (vital1 == 0 | vital1 >= 6)
display "marriages: check for 026 ERROR: Marital != 2, 3, but located wife is dead (vital1 == 1)"
replace error = "026 " + error if marital != 2 & marital != 3 & (w_id != . & wvital == 1) & (vital1 == 0 | vital1 >= 6)

write_error_file "../logs/Error report `date_time'" append
drop _all
display "marriages:exiting"
end

program define father_id
display "father_id: entering"
tempfile fathers save
tempvar f_id_found f_id_sum byear
local date_time = "`1'"
use ../combined/household_registers_raw

use dataset date clan hid id age rhhead sex using "../combined/household_registers_raw" if sex == 2
drop sex
rename rhhead rh_father
generate f_byear = date - age + 1
replace f_byear = 0 if age == 0
drop age
sort dataset date clan hid rh_father
rename id f_id
save "`fathers'"

use ../combined/household_registers_raw

local i = 1
local continue = 1
rename rhhead rh_father

generate `f_id_found' = 0
generate `f_id_sum' = 0
generate `byear' = date - age + 1
replace `byear' = 0 if age == 0

mk_f_rhhead rh_father rh_father
while (`continue' > 0) {
	sort dataset date clan hid rh_father
	merge dataset date clan hid rh_father using "`fathers'", nokeep
	drop _merge
	replace `f_id_found' = f_id != .
	replace `f_id_sum' = sum(`f_id_found')
	local continue = `f_id_sum'[_N]

	if (`continue' > 0) {
		rename f_id f_id_`i'
		rename f_byear f_byear_`i'
		mk_f_rhhead rh_father rh_father
		local i = `i' + 1
	}
}

local i = `i' - 1
drop f_id
compress f_id_*
mvencode f_id_* f_byear_*, mv(0) override

save "`save'"

display "father_id: checking error 031 born before paternal ancestor"
generate error = ""
forvalues j = 1/`i' {
	replace error = "031 " + error if `byear' < f_byear_`j' & `byear' != 0 & f_byear_`j' != 0
}
write_error_file "../logs/Error report `date_time'" append

display "father_id: checking error 029 inconsistency in father between current and previous register"
use "`save'"
keep if f_id_1 != 0
sort id date
by id: generate error = "029 " if _n > 1 & f_id_1 != f_id_1[_n-1] 
tab dataset if error == "029 "
write_error_file "../logs/Error report `date_time'" append

display "father_id: checking error 030 inconsistency in grandfather between current and previous register"
use "`save'"
keep if f_id_2 != 0
sort id date
by id: generate error = "030 " if _n > 1 & f_id_2 != f_id_2[_n-1] 
tab dataset if error == "030 "
write_error_file "../logs/Error report `date_time'" append
 
display "father_id: producing household_registers_ancestors"
use "`save'"
keep id date f_id_*
sort id date
by id date: keep if _n == 1
reshape long f_id_, i(id date) j(gen)
keep if f_id_ != 0
sort id gen date
by id gen: keep if _n == 1

keep gen id f_id_

reshape wide f_id_, i(id) j(gen)

save "../combined/household_registers_ancestors", replace
drop _all
display "father_id: exiting"
end

program define mother_id
display "mother_id: entering"
tempfile mothers save
tempvar m_id_found m_id_sum byear
local date_time = "`1'"
use ../combined/household_registers_raw

use marital dataset date clan hid id age rhhead sex using "../combined/household_registers_raw" if sex == 1 & marital != 2 
drop sex marital
mk_h_rhhead rhhead rh_mother
generate m_byear = date - age + 1
replace m_byear = 0 if age == 0
drop age
sort dataset date clan hid rh_mother
rename id m_id
save "`mothers'"

use ../combined/household_registers_raw

local i = 1
local continue = 1
rename rhhead rh_mother

generate `m_id_found' = 0
generate `m_id_sum' = 0
generate `byear' = date - age + 1
replace `byear' = 0 if age == 0

mk_f_rhhead rh_mother rh_mother
while (`continue' > 0) {
	sort dataset date clan hid rh_mother
	merge dataset date clan hid rh_mother using "`mothers'", nokeep
	drop _merge
	replace `m_id_found' = m_id != .
	replace `m_id_sum' = sum(`m_id_found')
	local continue = `m_id_sum'[_N]

	if (`continue' > 0) {
		rename m_id m_id_`i'
		rename m_byear m_byear_`i'
		mk_f_rhhead rh_mother rh_mother
		local i = `i' + 1
	}
}

local i = `i' - 1
drop m_id
compress m_id_*
mvencode m_id_* m_byear_*, mv(0) override

save "`save'"
 
display "mother_id: producing household_registers_ancestors"
use "`save'"
keep id date m_id_*
sort id date
by id date: keep if _n == 1
reshape long m_id_, i(id date) j(gen)
keep if m_id_ != 0
sort id gen date
by id gen: keep if _n == 1

keep gen id m_id_

reshape wide m_id_, i(id) j(gen)

sort id

merge id using "../combined/household_registers_ancestors", nokeep

sort id
save "../combined/household_registers_ancestors", replace

drop _merge

save "../combined/household_registers_ancestors", replace
drop _all
display "mother_id: exiting"
end

program define father_id_2
display "father_id_2: calculate father_id based on mother_id"
tempfile wives
use ../combined/household_registers_marriages
keep w_id h_id
sort w_id 
save "`wives'"
use ../combined/household_registers_ancestors
foreach x of varlist m_id_* {
	local t = substr("`x'",2,.)
	local y = "f`t'"
	rename `x' w_id
	sort w_id
	merge w_id using "`wives'", nokeep
	drop _merge
	capture generate int `y' = 0
	replace `y' = h_id if (`y' == . | `y' == 0) & h_id != .
	drop h_id
	rename w_id `x'
}	
mvencode _all, mv(0) override
save "../combined/household_registers_ancestors", replace
display "father_id_2: exiting"
end

program define father_id_3
display "father_id_3: assign a father_id based on ids of kin"
use ../combined/household_registers_ancestors
sort id
save, replace
use ../combined/household_registers_raw
tempvar no_f rhhead_f save_id change
sort id date
keep dataset case hid clan id date rhhead 
merge id using ../combined/household_registers_ancestors, nokeep
mvencode _all, mv(0) override
drop _merge
mk_f_rhhead rhhead `rhhead_f'
sort id
generate `save_id' = 0
generate byte `no_f' = 0
foreach x of varlist f_id_* {
	local continue_flag = 1
	while (`continue_flag') {
		replace `save_id' = `x'
		replace `no_f' = `x' == 0 | `x' == .
		sort dataset date clan hid `rhhead_f' `no_f' `x'	
		by dataset date clan hid `rhhead_f': replace `x' = `x'[1] 
		sort id `no_f' `x' 
		by id: replace `x' = `x'[1]
		replace `save_id' = `save_id' - `x'
		summ `save_id'
		local continue_flag = r(min)-r(max) 
		display "`continue_flag'"
	}
 	mk_f_rhhead `rhhead_f' `rhhead_f'
}
sort id date
by id: keep if _n == 1
keep id f_* m_*
mvencode _all, mv(0) override
compress
save ../combined/household_registers_ancestors, replace
display "father_id_3: exiting"
end

program define father_id_4
display "father_id_4: entering"
*set trace on
tempfile ancestors save previous
use ../combined/household_registers_ancestors
keep id f_id_*
reshape long f_id_, i(id) j(gen)
sort id f_id_
drop if id == f_id_
reshape wide f_id_, i(id) j(gen)
mvencode _all, mv(0) override
sort id
*brow
save "`ancestors'", replace
*brow
use ../combined/household_registers_ancestors
keep id f_id_*
reshape long f_id_, i(id) j(gen)
drop if f_id_ == 0
rename id original
sort original gen
*brow
save "`save'", replace
by original: keep if _n == _N
rename gen max_gen
rename f_id_ id
local continue_flag = 1
local file_count = 1
local old_updated_N = 0
while (`continue_flag') {
	sort id
	local continue_flag = 0
	merge id using "`ancestors'", nokeep
	sort original	
	*brow
	drop _merge
	summ f_id_1
	local updated_N = r(N)
	local updated_sd = r(sd)
	if ((`updated_sd' ~= 0 ) & (`updated_N' ~= `old_updated_N')) {
		local continue_flag = 1
		reshape long f_id_, i(original) j(gen)
		sort original gen
		replace gen = gen+max_gen
*		brow
		drop if f_id_ == 0 | f_id_ == . | id == f_id_ | original == f_id_	 	
		drop max_gen
		save "`previous'`file_count'", replace
		sort original gen
		by original: keep if _n == _N
		rename gen max_gen
		sort original
*		brow
		sort id
		drop id
		rename f_id_ id		
		local file_count = `file_count'+1
	}
	local old_updated_N = `updated_N'
}
drop _all
use "`save'"
while (`file_count' > 1) {
	local file_count = `file_count' - 1
	append using "`previous'`file_count'"
}
drop id
sort original gen
by original: generate founder_inferred= f_id_[_N]
sort original f_id_ gen
by original f_id: keep if (_n == 1) | f_id_ == 0 | f_id_ == .
sort original gen
by original: replace gen = _n

reshape wide f_id_, i(original) j(gen)
rename original id
sort id
save "`save'", replace
use "../combined/household_registers_ancestors"
drop f_id_*
capture drop founder_inferred
sort id
merge id using "`save'", nokeep
drop _merge
desc
mvencode _all, mv(0) override
sort id
replace founder_inferred = id if founder_inferred == 0
save, replace
drop _all
*set trace off
display "father_id_4: exiting"
end		

program define father_id_4a
display "father_id_4a: entering"
use ../combined/household_registers_ancestors
tempvar max_id

sort id
generate `max_id' = id[_N]

local j = 1
capture confirm numeric variable f_id_`j'
while (_rc == 0) {
	local j = `j'+1
	capture confirm numeric variable f_id_`j'
}
local j = `j'-1

local i = 1 
while (`i' <= `j') {
	replace f_id_`i' = -f_id_`i' if f_id_`i' > `max_id'
	local i = `i' + 1
}

local sort_string "f_id_1"
local i = 2
while (`i' <= `j') {
	local new_sort_string "`sort_string' f_id_`i'"
	capture sort `new_sort_string'
	capture by `sort_string': replace f_id_`i' = f_id_`i'[1]
	local sort_string "f_id_`i'"
	local i = `i' + 1
}

local i = 1 
while (`i' <= `j') {
	capture replace f_id_`i' = -f_id_`i' if f_id_`i' < 0
	local i = `i' + 1
}

drop `max_id'
sort id
save ../combined/household_registers_ancestors, replace
display "father_id_4a: exiting"
end


program define father_id_5
display "father_id_5: entering"
use ../combined/household_registers_ancestors
sort id
save, replace
tempvar n_rhhead one zero maxid block1 block2
tempfile save max save2 blocks
use ../combined/household_registers_raw
keep dataset date clan hid id date rhhead
sort id date
merge id using ../combined/household_registers_ancestors, nokeep
drop _merge
rename rhhead `n_rhhead'
generate `zero' = 0
sort dataset
save "`save'"
sort id
generate `maxid' = id[_N]
keep if _n == _N
keep `maxid'
save "`max'"
drop _all

local continue_flag = 1
local gen = 1
local id_avg = 0
while (`continue_flag') {
	use "`save'"
	local continue_flag = 0
	keep if f_id_`gen' == 0
	mk_f_rhhead `n_rhhead' `n_rhhead'
	sort dataset date clan hid `n_rhhead'
	by dataset date clan hid `n_rhhead': keep if _N > 1
	by dataset date clan hid `n_rhhead': generate `block1' = _N

	if (`gen' > 1) {
		sort date id
		merge date id using "`blocks'", nokeep
		drop _merge
		keep if `block1' != `block2'
		drop `block2'
	}
	
	summ dataset
	if (r(N) > 0) {
		drop `block1'
		save "`save'", replace

		sort dataset date clan hid `n_rhhead'
		by dataset date clan hid `n_rhhead': generate `block2' = _N
		sort date id
		keep date id `block2'
		save "`blocks'", replace	

		use "`save'"			
		merge using "`max'", nokeep
		replace `maxid' = `maxid'[1]
		drop _merge
		local continue_flag = 1
		sort dataset date clan hid `n_rhhead'
		by dataset date clan hid `n_rhhead': generate `one' = _n == 1
		replace f_id_`gen' = sum(`one') + `maxid'
		local continue_flag_2 = 1
		while(`continue_flag_2') {
			replace `zero' = f_id_`gen' == 0
			sort id `zero' f_id_`gen'
			by id: replace f_id_`gen' = f_id_`gen'[1]
			replace `zero' = f_id_`gen' == 0
			sort dataset date clan hid `n_rhhead' `zero'
			by dataset date clan hid `n_rhhead': replace f_id_`gen' = f_id_`gen'[1]
			summ f_id_`gen'
			local continue_flag_2 = int(r(mean)*1000) != `id_avg'
			display r(mean) " `id_avg' `continue_flag_2'"
			local id_avg = int(r(mean)*1000)
		}
		keep id date f_id_`gen'
		sort id date
		by id: keep if _n == 1
		sort id
		*brow
		drop date
		save "`save'`gen'"
		sort f_id_`gen'
		rename f_id_`gen' `maxid'
		keep if _n == _N
		keep `maxid'
		save "`max'", replace
		local gen = `gen' + 1
	}
	drop _all
}
use ../combined/household_registers_ancestors
sort id
mvdecode f_id_*, mv(0) 
local gen = `gen' - 1
display "`gen'"
while (`gen' > 0) {
	merge id using "`save'`gen'", nokeep update
	drop _merge
	local gen = `gen' - 1
	sort id
}
mvencode f_id_*, mv(0) override
reshape long f_id_, i(id) j(gen)
sort id f_id_ gen
by id f_id_: keep if (_n == 1) | f_id_ == 0 | f_id_ == .
sort id gen
by id: replace gen = _n

reshape wide f_id_, i(id) j(gen)

save "../combined/household_registers_ancestors", replace
drop _all
display "father_id_5: exiting"

end

program define father_id_6
display "father_id_6: entering"
* Work backward on relationships to identify most recent common ancestor for everyone in the household, then 
* copy over ancestor ids or create new ones accordingly .
tempvar ancestor done hsize unique matches count newid
tempfile save save2 maxid maxid2
use ../combined/household_registers_raw
keep dataset date id clan hid rhhead
sort date id
by date id: keep if _n == 1
*sort id
*merge id using ../combined/household_registers_ancestors, nokeep
*drop _merge
local continue_flag = 1
*generate `done' = 0
local gen = 1 
local gen2 = 2
rename rhhead rhhead_1
local save_count = 0
while (`continue_flag') {
	display "father_id_6: Inside loop"
	local continue_flag = 0
	mk_f_rhhead rhhead_`gen' rhhead_`gen2'
	sort dataset date clan hid
	by dataset date clan hid: generate `hsize' = _N
	reshape long rhhead_, i(date id) j(gen)
	sort dataset date clan hid rhhead_ id
	by dataset date clan hid rhhead_ id: generate byte `unique' = _n == 1
	by dataset date clan hid rhhead_: generate `matches' = sum(`unique')

	by dataset date clan hid rhhead_: generate `ancestor' = (`matches'[_N] == `hsize')
*	brow
	* Discard some problematic rhhead codings
	replace `ancestor' = `ancestor' | (dataset == 9 & clan == 135 & date >= 828)
	replace `ancestor' = `ancestor' | substr(rhhead_,1,4) == "3fyb" | substr(rhhead_,1,4) == "2fyb"
	replace `ancestor' = `ancestor' | (strlen(rhhead_) > 1 & substr(rhhead_,-2,2) == "fw")
	replace `ancestor' = `ancestor' | (substr(rhhead_,1,1) == "f" & substr(rhhead_,3,1) == "f" & substr(rhhead_,2,1) != "f")
	replace `ancestor' = `ancestor' | (strlen(rhhead_) > 2 & substr(rhhead_,-2,2) == "fm" & substr(rhhead_,-3,1) != "f")
	replace `ancestor' = `ancestor' | (strlen(rhhead_) > 1 & (substr(rhhead_,-1,1) == "f" | substr(rhhead_,-1,1) == "m") & (substr(rhhead_,-2,1) != "f" & substr(rhhead_,-2,1) != "m"))
 	sort dataset date clan hid `ancestor'
	by dataset date clan hid: generate `done' = `ancestor'[_N]
	sort dataset date clan hid rhhead_ id
	label var `unique' unique
	label var `matches' matches
	label var `hsize' hsize
*	brow
	drop `unique' `matches' `hsize'
 	summ `done'
	if (r(min) == 0) {
		local continue_flag = 1
	}
	save "`save'", replace
	keep if `done'
	sort date id gen
	by date id: keep if sum(`ancestor') == 0 | `ancestor' == 1
	drop `done' `ancestor'
	save "`save'`gen'", replace
	use "`save'"
	drop if `done'
	local gen = `gen'+1
	local gen2 = `gen2'+1
*	if (`save_count' == 0) {
*		summ `done'
*		local save_count = r(N) + 1
*	}
	summ `done'
	* Keep going as long as records are left, but there has been change since last iteration
	if (r(N) > 0 & `gen' < 15) {
*		local save_count = r(N)
		reshape wide rhhead_, i(date id) j(gen)
		drop `done' `ancestor'
	} 
	else {
		* Either we're out of records, or we're in a loop and the number of records hasn't changed since last time
		local continue_flag = 0
		drop _all
	}			
}
display "father_id_6: Loop complete"
use "../combined/household_registers_ancestors"
reshape long f_id_, i(id) j(gen)
keep id gen f_id_
sort id gen
save "`save2'", replace
keep if _n == _N
rename id maxid
keep maxid
save "`maxid'", replace
use "`save2'"
keep f_id_
sort f_id_
keep if _n == _N
rename f_id_ maxid2
save "`maxid2'", replace
local gen = `gen' - 1
use "`save'`gen'"
while (`gen' > 1) {
	local gen = `gen' - 1
	append using "`save'`gen'"
}	

replace gen = gen - 1
sort id gen
merge id gen using "`save2'", nokeep
drop _merge
merge using "`maxid'", nokeep
replace maxid = maxid[1]
drop _merge
sort dataset
merge using "`maxid2'", nokeep
replace maxid2 = maxid2[1]
drop _merge

replace f_id_ = id if gen == 0
replace f_id_ = 0 if f_id_ == .
generate byte no_id = f_id_ == 0 | f_id_ == .
sort dataset date clan hid rhhead_ no_id f_id_

display "First"

by dataset date clan hid rhhead_: generate byte `newid' = rhhead != "" & (f_id_[1] == f_id_[_N]) & (f_id_[1] == 0) 
by dataset date clan hid rhhead_: generate byte `count' = _n == 1 & `newid'
replace f_id_ = sum(`count')+maxid if `newid' 

replace no_id = f_id_== 0 | f_id_ == .
sort dataset date clan hid rhhead_ no_id f_id_

display "Second"
*by dataset date clan hid rhhead_: replace f_id_ = f_id_[1] if ((f_id_[1] < f_id_ & f_id_[1] <= maxid) | (f_id_ == 0))
by dataset date clan hid rhhead_: replace f_id_ = f_id_[1] if f_id_[1] < f_id_ 
*gsort dataset date clan hid rhhead_ no_id -f_id_
*by dataset date clan hid rhhead_: replace f_id_ = f_id_[1] if ((f_id_[1] < f_id_ & f_id_[1] > maxid) | (f_id_ == 0))

drop `newid' `count'
*sort id gen f_id_
sort id gen no_id f_id_
display "4.1"
*by id gen: replace f_id_ = f_id_[1] if ((f_id_[1] < f_id_ & f_id_[1] <= maxid) | (f_id_ == 0))
by id gen: replace f_id_ = f_id_[1] if f_id_[1] < f_id_ 
display "4.2"
sort dataset date clan hid rhhead_
display "Fifth"
*gsort id gen no_id -f_id_ 
display "5.1"
*by id gen: replace f_id_ = f_id_[1] if ((f_id_[1] > f_id_ & f_id_[1] > maxid) | (f_id_ == 0))
display "5.2"
sort dataset date clan hid rhhead_
display "Sixth"
drop no_id rhhead_ date clan hid dataset
sort id gen 
display "6.1"
by id gen: keep if _n == 1
reshape wide f_id_, i(id) j(gen)
drop f_id_0 maxid maxid2 
sort id
display "6.2"
save "`save'", replace
use "../combined/household_registers_ancestors"
mvdecode f_id_*, mv(0)
sort id
display "7.1"
merge id using "`save'", nokeep update
display "7.1"
mvencode f_id_*, mv(0) override
drop _merge
save "../combined/household_registers_ancestors", replace
drop _all
display "father_id_6: exiting"
end		

program define founder_ancestor
display "founder_ancestor: entering"
tempvar before done
tempfile maxid save husband
use ../combined/household_registers_marriages
rename w_id id
rename h_id founder_ancestor
sort id founder_ancestor
by id: keep if _n == 1
keep id founder_ancestor
sort id
save "`husband'", replace
use ../combined/household_registers_ancestors
sort id
keep if _n == _N
rename id maxid
keep maxid
save "`maxid'", replace
use ../combined/household_registers_ancestors
capture drop founder_ancestor
reshape long f_id_, i(id) j(gen)
merge using "`maxid'", nokeep
replace maxid = maxid[1]
drop _merge
generate byte within = f_id_ > 0 & f_id_ <= maxid
gsort id -gen
by id: generate within_cnt = sum(within)
by id: drop if within_cnt == 0
by id: keep if _n == 1
keep id f_id_
rename f_id_ founder_ancestor
sort id founder_ancestor
save "`save'", replace
use ../combined/household_registers_ancestors
capture drop founder_ancestor
sort id
merge id using "`save'", nokeep
drop _merge
mvdecode founder_ancestor, mv(0)
sort id
merge id using "`husband'", nokeep update
drop _merge
replace founder_ancestor = id if founder_ancestor == .
compress
foreach x of varlist f_id_* {
	generate `x'_a = (`x' > 0)
}
summ f_id_*_a
drop *_a
keep id founder_inferred founder_ancestor f_id_* m_id_*
save "../combined/household_registers_ancestors", replace
drop _all
display "founder_ancestor: exiting"
end

program define founder_group_1
display "founder_group_1: entering"
tempfile surnames household_registers_ancestors founder_s save

use "../combined/household_registers_ancestors"
sort id
keep id founder_inferred founder_ancestor f_id_* m_id_*
desc
save, replace

tempfile surnames
use "../combined/household_registers_raw" if sex == 2
keep if index(trim(name)," ")
replace name = trim(substr(trim(name),1,index(trim(name)," ")))
rename name surname_yihu
replace surname_yihu = "piao" if surname_yihu == "pu"
sort dataset date clan surname_yihu
by dataset date clan surname_yihu: generate count = _N
sort dataset date clan count
by dataset date clan: keep if _n == _N
sort dataset date clan
by dataset date: replace surname_yihu = surname_yihu[_n-1] if _n > 1 & surname_yihu == "" 
keep dataset date clan surname_yihu
sort dataset date clan
save "`surnames'"

use "../combined/household_registers_raw"
keep dataset date case interpol id clan  
sort id
merge id using "../combined/household_registers_ancestors", nokeep keep(founder_ancestor)
drop _merge

sort dataset date clan
merge dataset date clan using "`surnames'", nokeep
drop _merge
sort dataset date clan case interpol
by dataset date: generate new_surname = _n > 1 & surname_yihu != surname_yihu[_n-1] & surname_yihu != ""
by dataset date: generate surname_cnt = sum(new_surname)

sort founder_ancestor date clan
by founder_ancestor: generate founder_group_dataset = dataset[1]  
by founder_ancestor: generate founder_group_date = date[1]
by founder_ancestor: generate founder_group_yihu = clan[1]
by founder_ancestor: generate founder_group_surname = surname_yihu[1]

generate byte wrong_surname = 0
generate byte change = 0
local continue_flag = 1
local j = 0 
while (`continue_flag') {

	sort dataset date surname_cnt wrong_surname founder_group_dataset founder_group_date founder_group_yihu

	by dataset date surname_cnt: replace change = (founder_group_date[1] < founder_group_date) | ((founder_group_date[1] == founder_group_date) & (founder_group_dataset[1] == founder_group_dataset) & (founder_group_yihu[1] < founder_group_yihu))
*	brow if surname_yihu == "li" & dataset == 20
	by dataset date surname_cnt: replace founder_group_yihu = founder_group_yihu[1] if change
	by dataset date surname_cnt: replace founder_group_surname = founder_group_surname[1] if change
	by dataset date surname_cnt: replace founder_group_date = founder_group_date[1] if change
	by dataset date surname_cnt: replace founder_group_dataset = founder_group_dataset[1] if change

*	brow if surname_yihu == "li" & dataset == 20
	
	sort founder_ancestor founder_group_dataset founder_group_date founder_group_yihu
	by founder_ancestor: replace change = (surname_yihu == founder_group_surname[1]) & ((founder_group_date[1] < founder_group_date) | ((founder_group_date[1] == founder_group_date) & (founder_group_dataset[1] == founder_group_dataset) & (founder_group_yihu[1] < founder_group_yihu)))
	by founder_ancestor: replace founder_group_yihu = founder_group_yihu[1] if change
	by founder_ancestor: replace founder_group_surname = founder_group_surname[1] if change 
	by founder_ancestor: replace founder_group_date = founder_group_date[1] if change
	by founder_ancestor: replace founder_group_dataset = founder_group_dataset[1] if change
*	brow if surname_yihu == "li" & dataset == 20

	sort dataset date surname_cnt founder_group_dataset founder_group_date founder_group_yihu 
	by dataset date surname_cnt: generate byte not_done = (founder_group_date[1] < founder_group_date) | ((founder_group_date[1] == founder_group_date) & (founder_group_dataset[1] == founder_group_dataset) & (founder_group_yihu[1] < founder_group_yihu)) 
	summ not_done
*	brow
	local continue_flag = r(max)
	local j = `j'+1
	drop not_done
	sort dataset date surname_cnt case interpol
*	brow if dataset == 20
}
sort id date
by id: keep if _n == 1
keep id founder_group_dataset founder_group_date founder_group_yihu founder_group_surname
sort id
*brow if dataset == 20
save "`founder_s'"

use "../combined/household_registers_ancestors"
sort id
merge id using "`founder_s'"
drop _merge
sort id
*brow if dataset == 20
save "../combined/household_registers_ancestors", replace

sort founder_group_dataset founder_group_date founder_group_yihu
by founder_group_dataset founder_group_date founder_group_yihu: generate new = _n == 1
tab founder_group_dataset new
drop _all
display "founder_group_1: exiting"

end

program define founder_group_2
display "founder_group_2:entering"
tempfile temp

use ../combined/household_registers_raw
sort dataset address
merge dataset address using "Village master", nokeep keep(assigned_area assigned_name suoshu)
drop _merge
sort id
merge id using "../combined/household_registers_ancestors", nokeep keep(founder_group_dataset founder_group_surname founder_group_date founder_group_yihu f_id_* m_id_*)
drop _merge

keep if assigned_name != "" & assigned_name != "cheng nei" & !suoshu
sort founder_group_dataset founder_group_date founder_group_yihu assigned_area assigned_name
by founder_group_dataset founder_group_date founder_group_yihu assigned_area assigned_name: generate obs = _N
by founder_group_dataset founder_group_date founder_group_yihu assigned_area assigned_name: keep if _n == 1
gsort founder_group_dataset founder_group_date founder_group_yihu -obs
by founder_group_dataset founder_group_date founder_group_yihu: keep if _n == 1
gsort assigned_area assigned_name founder_group_dataset founder_group_surname -obs
*brow
by assigned_area assigned_name founder_group_dataset founder_group_surname: drop if _N == 1
by assigned_area assigned_name founder_group_dataset founder_group_surname: generate new_founder_group_date = founder_group_date[1]
by assigned_area assigned_name founder_group_dataset founder_group_surname: generate new_founder_group_dataset = founder_group_dataset[1]
by assigned_area assigned_name founder_group_dataset founder_group_surname: generate new_founder_group_yihu = founder_group_yihu[1]
keep founder_group_dataset founder_group_date founder_group_yihu new_founder_group_date new_founder_group_yihu new_founder_group_dataset
sort founder_group_dataset founder_group_date founder_group_yihu
save "`temp'"
use "../combined/household_registers_ancestors"
sort founder_group_dataset founder_group_date founder_group_yihu
merge founder_group_dataset founder_group_date founder_group_yihu using "`temp'", nokeep
drop _merge
replace founder_group_dataset = new_founder_group_dataset if new_founder_group_dataset != .
replace founder_group_date = new_founder_group_date if new_founder_group_date != .
replace founder_group_yihu = new_founder_group_yihu if new_founder_group_yihu != .
sort founder_group_dataset founder_group_date founder_group_yihu
by founder_group_dataset founder_group_date founder_group_yihu: generate new = _n == 1
tab founder_group_dataset new
drop new new_founder_group_dataset new_founder_group_date new_founder_group_yihu
sort id
save, replace
drop _all
display "founder_group_2:exiting"
end

program define founder_yihu
display "founder_yihu: entering"
tempfile surnames household_registers_ancestors founder_s save

use "../combined/household_registers_ancestors"
sort id
keep id founder_inferred founder_ancestor founder_group_dataset founder_group_surname founder_group_date founder_group_yihu f_id_* m_id_*
desc
save, replace

tempfile surnames
use "../combined/household_registers_raw" if sex == 2
keep if index(trim(name)," ")
replace name = trim(substr(trim(name),1,index(trim(name)," ")))
rename name surname_yihu
replace surname_yihu = "piao" if surname_yihu == "pu"
sort dataset date clan surname_yihu
by dataset date clan surname_yihu: generate count = _N
sort dataset date clan count
by dataset date clan: keep if _n == _N
sort dataset date clan
by dataset date: replace surname_yihu = surname_yihu[_n-1] if _n > 1 & surname_yihu == "" 
keep dataset date clan surname_yihu
sort dataset date clan
save "`surnames'"

use "../combined/household_registers_raw"
keep dataset date case interpol id clan  
sort id
merge id using "../combined/household_registers_ancestors", nokeep keep(founder_ancestor)
drop _merge

sort dataset date clan
merge dataset date clan using "`surnames'", nokeep
drop _merge

sort founder_ancestor date dataset clan  
by founder_ancestor: generate founder_yihu_date = date[1]
by founder_ancestor: generate founder_yihu_dataset = dataset[1]
by founder_ancestor: generate founder_yihu = clan[1]
by founder_ancestor: generate founder_yihu_surname = surname_yihu[1]

generate byte change = 0
local continue_flag = 1
while(`continue_flag') {

	sort dataset date clan founder_yihu_dataset founder_yihu_date founder_yihu

	by dataset date clan: replace change = (founder_yihu_date[1] < founder_yihu_date) | ((founder_yihu_date[1] == founder_yihu_date) & (founder_yihu_dataset[1] == founder_yihu_dataset) & (founder_yihu[1] < founder_yihu))
*	brow if surname_yihu == "li" & dataset == 20
	
	by dataset date clan: replace founder_yihu = founder_yihu[1] if change
	by dataset date clan: replace founder_yihu_surname = founder_yihu_surname[1] if change
	by dataset date clan: replace founder_yihu_date = founder_yihu_date[1] if change
	by dataset date clan: replace founder_yihu_dataset = founder_yihu_dataset[1] if change
*	brow if surname_yihu == "li" & dataset == 20
	
	sort founder_ancestor founder_yihu_dataset founder_yihu_date founder_yihu
	by founder_ancestor: replace change = (surname_yihu == founder_yihu_surname[1]) & ((founder_yihu_date[1] < founder_yihu_date) | ((founder_yihu_date[1] == founder_yihu_date) & (founder_yihu_dataset[1] == founder_yihu_dataset) & (founder_yihu[1] < founder_yihu)))
	by founder_ancestor: replace founder_yihu = founder_yihu[1] if change
	by founder_ancestor: replace founder_yihu_surname = founder_yihu_surname[1] if change 
	by founder_ancestor: replace founder_yihu_date = founder_yihu_date[1] if change
	by founder_ancestor: replace founder_yihu_dataset = founder_yihu_dataset[1] if change

*	brow if surname_yihu == "li" & dataset == 20

	sort dataset date clan founder_yihu_dataset founder_yihu_date founder_yihu 
	by dataset date clan: generate byte not_done = (founder_yihu_date[1] < founder_yihu_date) | ((founder_yihu_date[1] == founder_yihu_date) & (founder_yihu_dataset[1] == founder_yihu_dataset) & (founder_yihu[1] < founder_yihu)) 
	summ not_done
*	brow
	local continue_flag = r(max)
	drop not_done
	sort dataset date clan case interpol
*	brow if dataset == 20
}
sort id date
by id: keep if _n == 1
keep id founder_yihu_dataset founder_yihu_date founder_yihu founder_yihu_surname
sort id
*brow if dataset == 20
save "`founder_s'"

use "../combined/household_registers_ancestors"
sort id
merge id using "`founder_s'"
drop _merge
sort id
*brow if dataset == 20
save "../combined/household_registers_ancestors", replace

sort founder_yihu_dataset founder_yihu_date founder_yihu
by founder_yihu_dataset founder_yihu_date founder_yihu: generate new = _n == 1
tab founder_yihu_dataset new
drop _all

display "founder_yihu: exiting"

end

program define household_registers_generated_1
display "household_registers_generated_1: entering"
use ../combined/household_registers_raw
keep dataset date id case interpol clan hid age vital1 vital2 occu rank address disabil sex marital project recno
save_generated
drop _all
display "household_registers_generated_1: exiting"
end

program define household_registers_generated_2
display "household_registers_generated_2: entering"
tempfile surnames
use "../combined/household_registers_raw" if sex == 2
keep if index(trim(name)," ")
replace name = trim(substr(trim(name),1,index(trim(name)," ")))
rename name surname_yihu
replace surname_yihu = "piao" if surname_yihu == "pu"
sort dataset date clan surname_yihu
by dataset date clan surname_yihu: generate count = _N
sort dataset date clan count
by dataset date clan: keep if _n == _N
sort dataset date clan
by dataset date: replace surname_yihu = surname_yihu[_n-1] if _n > 1 & surname_yihu == "" 
keep dataset date clan surname_yihu 
sort dataset date clan
save "`surnames'"

use ../combined/household_registers_generated

sort dataset date clan
merge dataset date clan using "`surnames'"
drop _merge

sort id
merge id using "../combined/household_registers_ancestors", keep(f_id_1 f_id_2 m_id_1 m_id_2 founder_inferred founder_ancestor founder_group_yihu founder_group_dataset founder_group_date founder_group_surname founder_yihu_dataset founder_yihu_date founder_yihu founder_yihu_surname) nokeep
drop _merge

save_generated
display "household_registers_generated_2: exiting"
drop _all
end

program define household_registers_generated_3
*set trace on
set tracedepth 1

tempvar next_hh hh_new change_flag
display "household_registers_generated_3: entering"

use ../combined/household_registers_generated

generate byte popstatus=1
replace popstatus=4 if dataset==100 | dataset==101
replace popstatus=5 if dataset >= 100 & dataset <= 108
replace popstatus=6 if dataset > 108
replace popstatus=2 if dataset==3 | dataset==5 | dataset==21 | dataset==25
replace popstatus=3 if dataset==19 | dataset==24 | dataset==26 | dataset==27 | dataset==28

label define status1 1 "LN regular" 2 "LN specialized" 3 "LN lower status" 4 "SCP Jingqi" 5 "SCP Tunding" 6 "SCP Fuding"
label values popstatus status1
label variable popstatus "Population category"

sort id date

capture generate byte present = 0
label variable present "Alive and present in this register (vital1 == 0 | vital1 >= 6)"

capture generate byte disappear = 0
label variable disappear "Disappear after this register"

capture generate byte next_die = 0
label variable next_die "Die by next register (vital1 == 1 in next register)"

capture generate byte next_marry = 0
label variable next_marry "Marry out by next register (vital1 == 2 in next register)"

capture generate byte next_remarry = 0
label variable next_remarry "Remarry out by next register (vital1 == 3 in next register)"

capture generate byte next_tao = 0
label variable next_tao "Will become tao by next register (Vital1 == 4)" 

capture generate byte next3 = 0
label variable next3 "Next observation is 3 years away"

capture generate byte next6 = 0
label variable next6 "Next observation is 6 years away"

capture generate byte next1 = 0
label variable next1 "Next observation is 1 years away"

capture generate byte hh_size = 0
label variable hh_size "Number of living persons in household"

capture generate byte hh_divide_next = 0
label variable hh_divide_next "Number of hh into which hh will divide by next register"

capture generate byte hh_divide_prev = 0
label variable hh_divide_prev "Number of hh into which hh divided from previous register to current"

capture generate byte hh_mix_next = 0
label variable hh_mix_next "Number of hh with which hh will mix by next register"

capture generate byte hh_mix_prev = 0
label variable hh_mix_prev "Number of hh that current hh is mixed with"

capture generate long unique_hh_id = 0
label variable unique_hh_id "Unique houshold identifier"

replace present = (vital1 == 0 | vital1 >= 6) & (age > 0 & age != .)

by id: replace next1 = _n < _N & (date == date[_n+1] - 1)
by id: replace next3 = _n < _N & ((date == date[_n+1] - 3) | (date == date[_n+1] - 4))
by id: replace next6 = _n < _N & ((date == date[_n+1] - 6) | (date == date[_n+1] - 7))

by id: replace disappear = present & _n == _N

generate byte at_risk_die = present & ((next1 & dataset >= 100 & dataset <= 108) | (next3 & (dataset < 100 | dataset > 108)))

by id: replace next_die = present & _n < _N & vital1[_n+1] == 1

by id: generate newly_married = present & ((sex == 2 & _n > 1 & marital != 2 & marital[_n-1] == 2) | (sex == 1 & _n == 1 & marital !=2))

generate byte at_risk_marry = marital == 2 & present & ((next1 & dataset >= 100 & dataset <= 108) | (next3 & (dataset < 100 | dataset > 108)))

generate byte at_risk_remarry = marital == 3 & present & ((next1 & dataset >= 100 & dataset <= 108) | (next3 & (dataset < 100 | dataset > 108)))

by id: replace next_marry = present & ((sex == 1 & _n < _N &  vital1[_n+1] == 2) | (sex == 2 & marital == 2 & marital[_n+1] != . & marital[_n+1] != 0 & marital[_n+1] != 2))

by id: replace next_remarry = _n < _N & marital == 3 & ((sex == 1 & (vital1[_n+1] == 2 | vital1[_n+1] == 3)) | (sex == 2 & marital[_n+1] != 0 & marital[_n+1] != 3))

by id: replace next_tao = present & _n < _N & vital1[_n+1] == 4

generate `next_hh' = 0

by id: replace `next_hh' = dataset[_n+1]*100000+clan[_n+1]*100+hid[_n+1] if _n < _N

sort dataset date clan hid `next_hh'
by dataset date clan hid: replace hh_size = sum(present)
by dataset date clan hid: replace hh_size = hh_size[_N]

by dataset date clan hid: generate byte `hh_new' = (_n == 1 & `next_hh' != 0) | (_n > 1 & `next_hh' != `next_hh'[_n-1])
by dataset date clan hid: replace hh_divide_next = sum(`hh_new')
by dataset date clan hid: replace hh_divide_next = hh_divide_next[_N]

sort dataset date `next_hh' clan hid
by dataset date `next_hh': replace `hh_new' = _n == 1 | (_n > 1 & (clan != clan[_n-1] | hid != hid[_n-1]))
by dataset date `next_hh': replace hh_mix_next = sum(`hh_new')
by dataset date `next_hh': replace hh_mix_next = hh_mix_next[_N]

sort id date
by id: replace hh_divide_prev = hh_divide_next[_n-1] if _n > 1
by id: replace hh_mix_prev = hh_mix_next[_n-1] if _n > 1

sort dataset date clan hid hh_divide_prev
by dataset date clan hid: replace hh_divide_prev = hh_divide_prev[_N]
by dataset date clan hid: replace hh_mix_prev = hh_mix_prev[_N]

replace unique_hh_id = dataset*100000000+date*100000+clan*100+hid

sort id date

local changed = _N
local save_mean = 0
local continue_flag = 1
generate `change_flag' = 0

*keep if dataset <= 5

while (`continue_flag') {
	sort id date
	by id: replace unique_hh_id = unique_hh_id[_n-1] if _n > 1 & hh_mix_prev == 1 & hh_divide_prev == 1 & unique_hh_id != unique_hh_id[_n-1]
	sort dataset date clan hid unique_hh_id
	by dataset date clan hid: replace `change_flag' = (unique_hh_id != unique_hh_id[1]) & hh_divide_prev == 1 & hh_mix_prev == 1
*	brow if change_flag
	by dataset date clan hid: replace unique_hh_id = unique_hh_id[1] if `change_flag'
*	brow if change_flag
	summ `change_flag'
	local continue_flag = (r(mean) != 0) & (`changed' > int(r(mean)*_N))
	display `continue_flag' "  " r(mean) " " int(r(mean)*_N) " " `changed'
	local changed = int(r(mean)*_N)
}

drop __*
desc
save, replace
display "household_registers_generated_3: exiting"

set trace off
end

program define unique_address
display "unique_address: entering"
use ../combined/household_registers_generated
capture drop unique_village
capture drop area
capture drop location
capture drop suoshu
capture drop valid_village

sort dataset address
merge dataset address using "Village master", nokeep
drop _merge
sort assigned_area assigned_name
by assigned_area assigned_name: generate byte first = _n == 1
generate unique_village = sum(first)
replace unique_village = . if assigned_name == "" | suoshu
generate byte valid_village = unique_village != .
drop first
encode assigned_area, gen(area)
drop assigned_area assigned_name lati longi

replace area = 20 if dataset >= 100
replace area = 9 if dataset == 1 & area == . | area == 0
replace area = 2 if dataset == 2 & area == . | area == 0
replace area = 6 if dataset == 16 & area == . | area == 0
replace area = 7 if dataset == 19 & area == . | area == 0
replace area = 10 if dataset == 11 & area == . | area == 0
replace area = 99 if dataset == . 

generate byte location = 1
replace location = 2 if area == 1 | area == 5 | area == 9 | area == 11
replace location = 3 if area == 3 | area == 7 | area == 8 | area == 12
replace location = 4 if area == 2 | area == 4 | area == 13
replace location = 5 if area == 20
replace location = 99 if area == 99
label define loca 1 "North LN" 2 "Central LN" 3 "South central LN" 4 "South LN" 5 "SCP" 99 "Missing"
label value location loca

save_generated
drop _all

display "unique_address:exiting"

end

program define occupation_variables
display "occupation_variables: entering"
use "Rank master"
sort dataset rank
save, replace

use ../combined/household_registers_generated

generate byte tao = vital1 == 4
replace tao = . if !tao

capture drop income

describe, short

tab occu

merge m:1 dataset occu using "Occupation recodes", keep(match master)
drop _merge

tab occu

describe, short 

mvdecode soldier-honorif, mv(0)

* Set anything that is currently 0 to missing, in case there is information from ranks that might overwrite

merge m:1 dataset rank using "Rank recodes", update keep(match master match_update match_conflict)
drop _merge

describe, short

mvencode soldier-honorif, mv(0) override

replace tao = 0 if tao == .

sort id
by id: egen byte ever_tao = max(tao)
label variable ever_tao "Individual recorded as tao in any observation"

describe, short

mvencode soldier-honorif, mv(0) override

generate position = civil | banner | e | wei | hou_bu | ji_ming
label variable position "Salaried position"

generate income = yanglian + taels + rice/3.356 + beans/1.296

drop yanglian taels rice beans

save_generated
drop _all
display "occupation_variables: exiting"
end

program define fertility_variables
display "fertility_variables:entering"
tempfile fathers mothers

use ../combined/household_registers_generated
keep if age != 0 & (sex == 2 | marital == 2) & (birthyear > 700 & birthyear < 911)
sort id date
by id: keep if _n == 1
keep if f_id_1 != 0
keep f_id_1 birthyear sex
sort f_id_1 birthyear
by f_id_1: keep if _n < 20
by f_id_1: generate order = _n
rename birthyear birthyear_
reshape wide sex birthyear_, i(f_id_1) j(order) 
rename f_id_1 id
sort id 
mvencode _all, mv(0) override
save "`fathers'"

use ../combined/household_registers_generated
keep if age != 0 & (sex == 2 | marital == 2) & (birthyear > 700 & birthyear < 911)
sort id date
by id: keep if _n == 1
sort id
merge id using ../combined/household_registers_ancestors, keep(m_id_1) nokeep
drop _merge
keep if m_id_1 != 0
keep m_id_1 birthyear sex
sort m_id_1 birthyear
by m_id_1: keep if _n < 20
by m_id_1: generate order = _n
rename birthyear birthyear_
reshape wide sex birthyear_, i(m_id_1) j(order) 
rename m_id_1 id
sort id
mvencode _all, mv(0) override
save "`mothers'"

use ../combined/household_registers_generated
capture drop boys girls next_boys next_girls
sort id
merge id using "`fathers'", nokeep
drop _merge
sort id
merge id using "`mothers'", update nokeep
drop _merge
mvencode _all, mv(0) override
generate byte boys = 0
generate byte girls = 0
local i = 1
local continue = 1 
foreach x of varlist birthyear_* {
	display "`x'"
	local order = substr("`x'",11,.)
	display "`order'"
	local sex "sex`order'"
	replace boys = boys + 1 if `x' <= date & `sex' == 2
	replace girls = girls + 1 if `x' <= date & `sex' == 1
}
sort id date
by id: generate byte next_boys = boys[_n+1]-boys 
by id: generate byte next_girls = girls[_n+1]-girls
mvencode next_boys next_girls, mv(0) override
drop birthyear_*
rename sex savesex
drop sex*
rename savesex sex
sort id date
save ../combined/household_registers_generated, replace
display "fertility_variables: exiting"
end

program define kin_variables
display "kin_variables:entering"
use ../combined/household_registers_generated

capture drop birth_order birth_order_sex seniority seniority_sex
capture drop first

generate int birthyear = date - age + 1

sort id date
by id: generate byte first = f_id_1 != 0 & _n == 1 & (sex == 2 | (sex == 1 & (marital == 2 | (marital == 1 & vital1 == 2))))
by id: generate byte usable = f_id_1 != 0 & (sex == 2 | (sex == 1 & (marital == 2 | (marital == 1 & vital1 == 2))))

generate byte birth_order = 0
generate byte birth_order_sex = 0
generate byte seniority = 0
generate byte seniority_sex = 0

sort f_id_1 first birthyear
by f_id_1 first: replace birth_order = _n if first
sort f_id_1 first sex birthyear
by f_id_1 first sex: replace birth_order_sex = _n if first

sort id date

by id: replace birth_order = birth_order[1]
by id: replace birth_order_sex = birth_order_sex[1]

sort date f_id_1 usable birthyear
by date f_id_1 usable: replace seniority = _n if usable 

sort date f_id_1 usable sex birthyear
by date f_id_1 usable sex: replace seniority_sex = _n if usable 

drop usable first

save ../combined/household_registers_generated, replace
drop _all
display "kin_variables: exiting"
end

program define name_variables
display "name_variables:entering"
tempfile name_variables save

* Let's code the names

use "../combined/household_registers_raw" if sex == 2

generate byte has_surname = index(trim(name)," ") > 0

rename name given

replace given = trim(substr(trim(given),index(trim(given)," ")+1,.)) if index(trim(given)," ") > 0

replace given = trim(substr(trim(given),1,index(trim(given),"(")-1)) if index(trim(given),"(") > 0
replace given = subinstr(given,")","",.)

sort given

merge m:1 given using names, update replace keep(master match)
drop _merge

desc

save "`save'"

keep dataset recno has_surname non_han_name diminutive rustic number

sort dataset recno

save "`name_variables'"

use "`save'"

keep if non_han_name == .
sort given
by given: keep if _n == 1
save "Unassigned names", replace

use ../combined/household_registers_generated

sort dataset recno
merge dataset recno using "`name_variables'", nokeep update replace
drop _merge

mvencode non_han_name has_surname number rustic diminutive, mv(0) override
save_generated
drop _all
display "name_variables: exiting"

end

program define mk_h_rhhead
tempvar temp1 
generate `temp1' = rtrim(`1')
generate `2' = subinstr(`temp1',"w","",.) if (`temp1' != "w" & substr(`temp1',-1,1) == "w")
replace `2' = subinstr(`temp1',"q","",.) if (`temp1' != "q" & substr(`temp1',-1,1) == "q")

replace `2' = substr(`2', 1, strlen(`2')-1) if substr(`temp1',-1,1) == "w" & indexnot(substr(`2',-1,1),"0123456789") == 0

replace `2' = "e" if `temp1' == "w" | `temp1' == "q" | ((substr(`temp1',2,1) == "w" | substr(`temp1',2,1) == "q") & indexnot(substr(`temp1',1,1),"0123456789") == 0)

replace `2' = subinstr(`temp1',"m","f",.) if substr(`temp1',-1,1) == "m"
end

program define mk_f_rhhead
tempvar t
generate str16 `t' = rtrim(subinstr(`1',"w","",.))
replace `t' = rtrim(subinstr(`t',"c","",.))
replace `t' = substr(`t', 1, strlen(`t')-1) if indexnot(substr(`t',-1,1),"0123456789") == 0
replace `t' = "b" if `t' == ""
replace `t' = subinstr(`t',"e","b",.)
replace `t' = subinstr(`t',"m","f",.)

if ("`1'" == "`2'") {
	drop `1'
}

generate str16 `2' = substr(`t',1,length(`t')-2) if (substr(reverse(`t'),1,1) == "s" | substr(reverse(`t'),1,1) == "d") & real(substr(reverse(`t'),2,1)) != .
replace `2' = substr(`t',1,length(`t')-1) if (substr(reverse(`t'),1,1) == "s" | substr(reverse(`t'),1,1) == "d") & real(substr(reverse(`t'),2,1)) == .

replace `2' = substr(`t',1,length(`t')-2)+"f" if (substr(reverse(`t'),1,1) == "b" | substr(reverse(`t'),1,1) == "z") & real(substr(reverse(`t'),3,1)) == .
replace `2' = substr(`t',1,length(`t')-3)+"f" if (substr(reverse(`t'),1,1) == "b" | substr(reverse(`t'),1,1) == "z") & real(substr(reverse(`t'),3,1)) != .

replace `2' = `t'+"f" if substr(reverse(`t'),1,1) == "f" 
replace `2' = "e" if `2' == ""
replace `2' = subinstr(`2',"of","f",.)
replace `2' = subinstr(`2',"yf","f",.)
end

program define load_errors
display "load_errors: entering"
use ../combined/household_registers_raw
sort dataset recno
merge dataset recno using "../logs/Error report `1'", nokeep
drop _merge
display "load_errors: exiting"
end

program define siblings_cousins

display "Entering: siblings_cousins arguments: `1' `2'"

*set trace on
tempfile save_file current father_gen father_kin father mother max_id_file dataset_date yob ln scp child_hh blank_obs hukouce

tempvar male female temp max_id valid birth_year age save_id

local prefix "kin"
local sort ""
if ("`1'" == "hh") {
	local sort "clan hid"
	local prefix "kin_hh"
}

if ("`2'" ~= "") {
	local prefix "`prefix'_`2'"
}

* Only include sisters in counts, not sisters-in-law

display "siblings_cousins: saving save_file"
save "`save_file'"

* Create placeholder observations for individuals in all of their kids' households

keep if f_id_1 != 0 & f_id_1 != .
keep f_id_1 date clan hid dataset
sort f_id_1 date dataset clan hid
by f_id_1 date dataset clan hid: keep if _n == 1
rename f_id_1 id
sort id date dataset clan hid
display "siblings_cousins: saving child_hh"
save "`child_hh'", replace

use id date age using "`save_file'" if age != 0 & age != .
sort id date
by id:keep if _n == 1
generate `birth_year' = date-age
keep id `birth_year'
sort id
display "siblings_cousins: saving birth_year"
save "`yob'"

use dataset date using "`save_file'"
sort dataset date
by dataset date: keep if _n ==1
sort dataset date 
generate byte `valid' = 1
display "siblings_cousins: saving dataset_date"
save "`dataset_date'"

use id using "`save_file'"
sort id
keep if _n == _N
rename id `max_id'
display "siblings_cousins: saving max_id_file"
save "`max_id_file'"

*set trace on
use dataset using "`save_file'"
bysort dataset: keep if _n == 1
list dataset
save "`hukouce'"
local i = 1 
local j = dataset[`i']
while (`i' <= _N) {
	display `j'
	use dataset id date using "`save_file'" if dataset == `j'
	fillin dataset date id

	merge using "`max_id_file'", nokeep
	replace `max_id' = `max_id'[1]
	drop _merge

	keep if id <= `max_id'
	drop `max_id'

	sort dataset date
	merge dataset date using "`dataset_date'", nokeep
	drop _merge
	drop if `valid' == .

	if ("`sort'" ~= "") {
		sort id date
		merge id date using "`child_hh'", nokeep
		drop _merge
	}
	save "`blank_obs'`j'"
	local i = `i' + 1
	use "`hukouce'"
	local j = dataset[`i']
}

display "siblings_cousins: blanks created"

set trace on
use "`hukouce'" if dataset < 100
local i = 1
local j = dataset[`i']
local n = _N
while (`i' <= _N) {
	local j`i' = dataset[`i']
	local i = `i'+1
}

local i = 1
local j = `j`i''
while (`i' <= `n') {
	local j = `j`i''
	if (`i' == 1) {
		use "`blank_obs'`j'"
	} 
	else {
		append using "`blank_obs'`j'"
	}
	erase "`blank_obs'`j'"
	local i = `i'+1
}
sort id date `sort'
display "siblings_cousins: saving ln"
save "`ln'"

use "`hukouce'" if dataset > 100
local i = 1
local j = dataset[`i']
local n = _N
while (`i' <= _N) {
	local j`i' = dataset[`i']
	local i = `i'+1
}

local i = 1
local j = `j`i''
while (`i' <= `n') {
	local j = `j`i''
	if (`i' == 1) {
		use "`blank_obs'`j'"
	} 
	else {
		append using "`blank_obs'`j'"
	}
	erase "`blank_obs'`j'"
	local i = `i'+1
}
sort id date `sort'
display "siblings_cousins: saving scp"
save "`scp'"
set trace off

*desc

use dataset date id age male female clan hid `sort' vital1 recno using "`save_file'"

sort id date `sort'
merge id date `sort' using "`ln'"
drop _merge

sort id date `sort'
merge id date `sort' using "`scp'"
drop _merge

sort id 
merge id using "../combined/household_registers_ancestors", nokeep keep(f_id_1 f_id_2 f_id_3 f_id_4 m_id_1)
drop _merge

sort id
merge id using "`yob'", nokeep
drop _merge
generate `age' = date - `birth_year'

replace male = 0 if male == .
replace female = 0 if female == .

generate byte `male' = male
generate byte `female' = female

local i = 1

while (`i' <= 4) {

display "siblings_cousins: counts `i'"
* Include recno in sort just to make sure people appear in the same order every time, so as not to screw 
* up older and younger counts when there are people of the same age.

	sort date f_id_`i' `sort' `age' recno

	replace `male' = 0 if f_id_`i' == 0 | f_id_`i' == .
	replace `female' = 0 if f_id_`i' == 0 | f_id_`i' == .

	by date f_id_`i' `sort': egen byte `prefix'_m_`i' = total(`male')
	by date f_id_`i' `sort': egen byte `prefix'_f_`i' = total(`female')
 
	by date f_id_`i' `sort': generate byte `prefix'_ym_`i' = sum(`male')
	by date f_id_`i' `sort': generate byte `prefix'_yf_`i' = sum(`female')

	by date f_id_`i' `sort': generate byte `prefix'_om_`i' = `prefix'_m_`i' - `prefix'_ym_`i'
	by date f_id_`i' `sort': generate byte `prefix'_of_`i' = `prefix'_f_`i' - `prefix'_yf_`i'

*	desc
	local j = `i'
	local i = `i' + 1

}

*summ

local i = 4

while (`i' > 1) {
	local j = `i' - 1		
	replace `prefix'_m_`i' = `prefix'_m_`i' - `prefix'_m_`j' if f_id_`i' != 0 & f_id_`i' != . 
	replace `prefix'_f_`i' = `prefix'_f_`i' - `prefix'_f_`j' if f_id_`i' != 0 & f_id_`i' != . 
	replace `prefix'_om_`i' = `prefix'_om_`i' - `prefix'_om_`j' if f_id_`i' != 0 & f_id_`i' != . 
	replace `prefix'_of_`i' = `prefix'_of_`i' - `prefix'_of_`j' if f_id_`i' != 0 & f_id_`i' != . 
	replace `prefix'_ym_`i' = `prefix'_ym_`i' - `prefix'_ym_`j' if f_id_`i' != 0 & f_id_`i' != . 
	replace `prefix'_yf_`i' = `prefix'_yf_`i' - `prefix'_yf_`j' if f_id_`i' != 0 & f_id_`i' != . 

	local i = `i' - 1
}

replace `prefix'_m_1 = `prefix'_m_1 - male if f_id_1 != 0 & f_id_1 != .
replace `prefix'_f_1 = `prefix'_f_1 - female if f_id_1 != 0 & f_id_1 != .

replace `prefix'_ym_1 = `prefix'_ym_1 - male if f_id_1 != 0 & f_id_1 != .
replace `prefix'_yf_1 = `prefix'_yf_1 - female if f_id_1 != 0 & f_id_1 != .

*summ

keep id date `prefix'* `sort'

sort id date `sort'
by id date `sort': keep if _n == 1
sort id date `sort'

display "siblings_cousins: saving basic file"

save "`current'", replace

drop *_4

foreach x of varlist `prefix'* {
	rename `x' `x'_pa
}

local i = 3
while (`i' >= 1) {
	local j = `i' + 1
	rename `prefix'_m_`i'_pa `prefix'_m_`j'_pa
	rename `prefix'_f_`i'_pa `prefix'_f_`j'_pa
	rename `prefix'_ym_`i'_pa `prefix'_ym_`j'_pa
	rename `prefix'_yf_`i'_pa `prefix'_yf_`j'_pa
	rename `prefix'_om_`i'_pa `prefix'_om_`j'_pa
	rename `prefix'_of_`i'_pa `prefix'_of_`j'_pa
	local i = `i' - 1
}

rename id f_id_1

sort f_id_1 date `sort'
by f_id_1 date `sort': keep if _n == 1
sort f_id_1 date `sort'

save "`father_kin'", replace

use "`save_file'"
keep if male & (vital1 == 0 | vital1 > 6)
keep date id male `sort'
rename male `prefix'_m_1_pa
rename id f_id_1
sort f_id_1 date `sort'
by f_id_1 date `sort': keep if _n == 1
sort f_id_1 date `sort'
save "`father'", replace

use "`save_file'"
keep if female & (vital1 == 0 | vital1 > 6)
keep date id female `sort'
rename female `prefix'_f_1_ma
rename id m_id_1
sort m_id_1 date `sort'
by m_id_1 date `sort': keep if _n == 1
sort m_id_1 date `sort'
save "`mother'", replace

use ../combined/household_registers_marriages
sort w_id date
save, replace

use "`save_file'"

display "Siblings_cousins: Read saved file back in"

desc, short
generate `save_id' = id
rename id w_id
sort w_id date
merge w_id date using ../combined/household_registers_marriages, nokeep keep(h_id)
drop _merge

sort dataset recno
by dataset recno: keep if _n == 1

rename w_id id
replace id = h_id if sex == 1 & h_id != . & h_id != 0

sort id date `sort' 
merge id date `sort' using "`current'", nokeep
drop _merge

display "Siblings_cousins: Merged in basic variables"
desc, short

replace id = `save_id'

desc, short

sort f_id_1 date `sort'
merge f_id_1 date `sort' using "`father_kin'", nokeep
drop _merge

display "Siblings_cousins: read in father's kin variables"
desc, short

sort f_id_1 date `sort'
merge f_id_1 date `sort' using "`father'", nokeep
drop _merge

display "Siblings_cousins: read in father's information"
desc, short

sort m_id_1 date `sort'
merge m_id_1 date `sort' using "`mother'", nokeep
drop _merge

display "Siblings_cousins: read in mothers's information"
desc, short

mvencode `prefix'*, mv(0) override

*set trace off
display "Siblings_cousins: exiting"
end

program define kin_counts

display "Inside: kin_counts"

use "../combined/household_registers_ancestors"
compress
save, replace

use dataset date id age clan hid vital1 marital sex f_id_1 m_id_1 recno using "../combined/household_registers_generated"

* Uncomment the following for testing
*keep if dataset == 1 | dataset == 101

compress

sort id
generate max_id = id[_N]
compress max_id
generate byte male = sex == 2 & (vital1 == 0 | vital1 >= 6) & (age != . & age > 0) 
generate byte female = sex == 1 & marital == 2 & (vital1 == 0 | vital1 >= 6) & (age != . & age > 0)
siblings_cousins all
siblings_cousins hh
keep id date dataset f_id_* kin_* recno

label variable kin_m_1 "# living male siblings"
label variable kin_f_1 "# living unmarried female siblings"
label variable kin_ym_1 "# living younger male siblings"
label variable kin_yf_1 "# living unmarried younger female siblings"
label variable kin_om_1 "# living older male siblings"
label variable kin_of_1 "# living unmarried older female siblings"

label variable kin_m_2 "# living male cousins"
label variable kin_f_2 "# living unmarried female cousins"
label variable kin_ym_2 "# living younger male cousins"
label variable kin_yf_2 "# living unmarried younger female cousins"
label variable kin_om_2 "# living older male cousins"
label variable kin_of_2 "# living unmarried older female cousins"

label variable kin_m_3 "# living male second cousins"
label variable kin_f_3 "# living unmarried female second cousins"
label variable kin_ym_3 "# living younger male second cousins"
label variable kin_yf_3 "# living unmarried younger female second cousins"
label variable kin_om_3 "# living older male second cousins"
label variable kin_of_3 "# living unmarried older female second cousins"

label variable kin_m_4 "# living male third cousins"
label variable kin_f_4 "# living unmarried female third cousins"
label variable kin_ym_4 "# living younger male third cousins"
label variable kin_yf_4 "# living unmarried younger female third cousins"
label variable kin_om_4 "# living older male third cousins"
label variable kin_of_4 "# living unmarried older female third cousins"

label variable kin_m_2_pa "# living father's brothers"
label variable kin_f_2_pa "# living father's unmarried sisters"
label variable kin_ym_2_pa "# living father's yonger brothers"
label variable kin_yf_2_pa "# living unmarried father's younger brothers"
label variable kin_om_2_pa "# living father's older brothers"
label variable kin_of_2_pa "# living unmarried father's younger sisters"

label variable kin_m_3_pa "# living father's cousins"
label variable kin_f_3_pa "# living father's unmarried cousins"
label variable kin_ym_3_pa "# living father's yonger cousins"
label variable kin_yf_3_pa "# living unmarried father's younger cousins"
label variable kin_om_3_pa "# living father's older cousins"
label variable kin_of_3_pa "# living unmarried father's younger cousins"

label variable kin_m_4_pa "# living father's second cousins"
label variable kin_f_4_pa "# living father's unmarried female second cousins"
label variable kin_ym_4_pa "# living father's younger male second cousins"
label variable kin_yf_4_pa "# living unmarried father's younger female second cousins"
label variable kin_om_4_pa "# living father's older male second cousins"
label variable kin_of_4_pa "# living unmarried father's older female second cousins"

label variable kin_hh_m_1 "# living male siblings in household"
label variable kin_hh_f_1 "# living unmarried female siblings in household"
label variable kin_hh_ym_1 "# living younger male siblings in household"
label variable kin_hh_yf_1 "# living unmarried younger female siblings in household"
label variable kin_hh_om_1 "# living older male siblings in household"
label variable kin_hh_of_1 "# living unmarried older female siblings in household"

label variable kin_hh_m_2 "# living male cousins in household"
label variable kin_hh_f_2 "# living unmarried female cousins in household"
label variable kin_hh_ym_2 "# living younger male cousins in household"
label variable kin_hh_yf_2 "# living unmarried younger female cousins in household"
label variable kin_hh_om_2 "# living older male cousins in household"
label variable kin_hh_of_2 "# living unmarried older female cousins in household"

label variable kin_hh_m_3 "# living male second cousins in household"
label variable kin_hh_f_3 "# living unmarried female second cousins in household"
label variable kin_hh_ym_3 "# living younger male second cousins in household"
label variable kin_hh_yf_3 "# living unmarried younger female second cousins in household"
label variable kin_hh_om_3 "# living older male second cousins in household"
label variable kin_hh_of_3 "# living unmarried older female second cousins in household"

label variable kin_hh_m_4 "# living male third cousins in household"
label variable kin_hh_f_4 "# living unmarried female third cousins in household"
label variable kin_hh_ym_4 "# living younger male third cousins in household"
label variable kin_hh_yf_4 "# living unmarried younger female third cousins in household"
label variable kin_hh_om_4 "# living older male third cousins in household"
label variable kin_hh_of_4 "# living unmarried older female third cousins in household"

label variable kin_hh_m_2_pa "# living father's brothers in household"
label variable kin_hh_f_2_pa "# living father's unmarried sisters"
label variable kin_hh_ym_2_pa "# living father's younger brothers in household"
label variable kin_hh_yf_2_pa "# living unmarried father's younger sisters in household"
label variable kin_hh_om_2_pa "# living father's older brothers in household"
label variable kin_hh_of_2_pa "# living unmarried father's older sisters in household"

label variable kin_hh_m_3_pa "# living father's cousins in household"
label variable kin_hh_f_3_pa "# living father's unmarried female cousins in household"
label variable kin_hh_ym_3_pa "# living father's younger male cousins in household"
label variable kin_hh_yf_3_pa "# living unmarried father's younger female cousins in household"
label variable kin_hh_om_3_pa "# living father's older male cousins in household"
label variable kin_hh_of_3_pa "# living unmarried father's older female cousins in household"

label variable kin_hh_m_4_pa "# living father's second cousins in household"
label variable kin_hh_f_4_pa "# living father's unmarried female second cousins in household"
label variable kin_hh_ym_4_pa "# living father's younger male second cousins in household"
label variable kin_hh_yf_4_pa "# living unmarried father's younger female second cousins in household"
label variable kin_hh_om_4_pa "# living father's older male second cousins in household"
label variable kin_hh_of_4_pa "# living unmarried father's older female second cousins in household"

save "../combined/household_registers_kin_counts_all", replace
drop _all

use dataset date id age clan hid vital1 marital sex f_id_1 m_id_1 recno using "../combined/household_registers_generated"

* Uncomment the following for testing
* keep if dataset == 1 | dataset == 101

compress
sort id
generate max_id = id[_N]
compress max_id
generate byte male = sex == 2 & (marital == 1 | marital >= 4) & (vital1 == 0 | vital1 >= 6) & (age != . & age != 0)
generate byte female = sex == 1 & (marital == 1 | marital >= 4) & (vital1 == 0 | vital1 >= 6) & (age != . & age != 0)
siblings_cousins all md
siblings_cousins hh md
keep id date dataset f_id_* kin_* recno

save "../combined/household_registers_kin_counts_married", replace

use dataset date id age clan hid vital1 marital sex f_id_1 m_id_1 recno using "../combined/household_registers_generated"

* Uncomment the following for testing
* keep if dataset == 1 | dataset == 101

compress
sort id
generate max_id = id[_N]
compress max_id
generate byte male = sex == 2 & (marital == 3) & (vital1 == 0 | vital1 >= 6) & (age != . & age != 0)
generate byte female = sex == 1 & (marital == 3) & (vital1 == 0 | vital1 >= 6) & (age != . & age != 0)
siblings_cousins all wid
siblings_cousins hh wid
keep id date dataset f_id_* kin_* recno
save "../combined/household_registers_kin_counts_widowed", replace

end

program define generate_inventory
display "generate_inventory: start"

tempfile supplements dong7 dong9

/* 

We are going to have to handle the Dong family as a special case, since the diaochabiao is linked to the genealogy, which is in turned linked to the hukouce. 

*/

use ../supplements/liaoning_supplements
keep if inventory == 7 & scase != 0 & sdate != 0
mvencode _all, mv(0) override
keep inventory case sdataset scase sdate sinterpol
rename inventory dsource
rename case dcase
sort dsource dcase
save "`dong7'"
drop _all

use ../supplements/liaoning_inventory
sort inventory
save, replace

use ../supplements/liaoning_supplements
mvencode _all, mv(0) override

sort dsource dcase

merge dsource dcase using "`dong7'", nokeep update replace
drop _merge

keep if source == 2 & sdataset != 0 & sdate != 0 & scase != 0

keep inventory case sdataset sdate scase sinterpol

tab inventory

rename case line
rename sdataset dataset
rename sdate date
rename scase case
rename sinterpol interpol
sort dataset date case interpol inventory line

by dataset date case interpol inventory: keep if _n == 1
by dataset date case interpol: generate source = _n

reshape wide inventory line, i(dataset date case interpol) j(source)

sort dataset date case interpol

save "`supplements'", replace

use ../combined/household_registers_generated if dataset < 100

capture drop inventory inv_surname

sort dataset date case interpol

merge dataset date case interpol using "`supplements'", nokeep
drop _merge

mvencode inventory*, mv(0) override
keep if inventory1 != 0

sort inventory1 line1

rename inventory1 inventory

sort inventory
merge inventory using "../supplements/liaoning_inventory", keep(inv_surname) nokeep

drop _merge

keep dataset founder_yihu_date founder_yihu inventory founder_yihu_surname inv_surname

sort dataset founder_yihu_date founder_yihu inventory

by dataset founder_yihu_date founder_yihu inventory: keep if _n == 1

by dataset founder_yihu_date founder_yihu: drop if _N > 1 & founder_yihu_surname != inv_surname

sort dataset founder_yihu_date founder_yihu

list

save "`supplements'", replace

use ../combined/household_registers_generated

capture drop inventory inv_surname

sort dataset founder_yihu_date founder_yihu

merge dataset founder_yihu_date founder_yihu using "`supplements'", nokeep

drop _merge

mvencode inventory, mv(0) override

save, replace

drop _all
display "generate_inventory: done"

end

program define finalize_error_report 
display "finalize_error_report: entering"

use "Error messages"

sort error
save, replace
drop _all

local date_time = "`1'"

use "../logs/Error report `date_time'"

keep dataset recno error
by dataset recno error: keep if _n == 1

destring error, replace
sort error

sort dataset recno error

by dataset recno: generate count = _n

capture label drop error_messages
error_labels
label values error error_messages

reshape wide error, i(dataset recno) j(count)

sort dataset recno

save "../logs/Error report `date_time'", replace

drop _all
display "finalize_error_report: exiting"
end

program define merged_error_report
display "merged_error_report: entering"
use ../combined/household_registers_raw
sort dataset_ck recno
desc
save, replace
use "../logs/Error report `1'"
sort dataset_ck recno
desc
merge dataset_ck recno using ../combined/household_registers_raw, nokeep
drop _merge
keep dataset_ck date_ck line_ck recno case date interpol rhhead name error* has_note
order dataset recno date_ck line_ck case date interpol rhhead name

sort dataset_ck date_ck line_ck
*merge dataset_ck date_ck line_ck using "../combined/Household register entries with notes", nokeep
*drop _merge

keep if has_note == 0

display "Numbers of observations in each dataset flagged for errors"
tab dataset

sort dataset_ck recno

save "../logs/Error report `1'", replace
outsheet using "../logs/Error report `1'.txt", replace
drop _all

use ../combined/household_registers_raw
sort dataset_ck recno
merge dataset_ck recno using "../logs/Error report `1'", nokeep
drop _merge

generate byte has_error = error1 != .

display "Proportions of observations in each dataset flagged for errors"
tab dataset, sum(has_error)

drop _all

display "merged_error_report: exiting"
end

program define error_labels
label define error_messages 1 `"001 ERROR: Invalid sex"', modify
label define error_messages 2 `"002 ERROR: Blank relationship"', modify
label define error_messages 3 `"003 ERROR: Sex = 2 but rh ends in w"', modify
label define error_messages 4 `"004 ERROR: Sex = 2 but rh ends in m"', modify
label define error_messages 5 `"005 ERROR: Sex = 2 but rh ends in d"', modify
label define error_messages 6 `"006 ERROR: Sex = 1, marital != 2, vital1 != 2, but rh does not end in w, c or m"', modify
label define error_messages 7 `"007 ERROR: Sex = 1, marital = 2, but rh does not end in d, z, or e"', modify
label define error_messages 8 `"008 ERROR: Duplicate case, date, interpol"', modify
label define error_messages 9 `"009 ERROR: Duplicate lcase, ldate, linterpol, but vital1 != 8"', modify
label define error_messages 10 `"010 ERROR: Unable to link based on lcase, ldate, linterpol"', modify
label define error_messages 11 `"011 ERROR: Ldate is greater than or equal to date"', modify
label define error_messages 12 `"012 ERROR: Change in clan, but rh != e"', modify
label define error_messages 13 `"013 ERROR: Change in hid, but rh != e"', modify
label define error_messages 14 `"014 ERROR: Rh = e, but no change in hid"', modify
label define error_messages 15 `"015 ERROR: Clan doesn't change, but hid decreases"', modify
label define error_messages 16 `"016 ERROR: Zu decreases"', modify
label define error_messages 17 `"017 ERROR: Marital != 2, 3, vital1 != 2, but no husband located"', modify
label define error_messages 18 `"018 ERROR: Marital = 2, but husband located"', modify
label define error_messages 19 `"019 ERROR: Marital = 3, but husband located and not dead"', modify
label define error_messages 20 `"020 ERROR: Wife matched to more than one husband in same register"', modify
label define error_messages 21 `"021 ERROR: Wife's husband ID appears to change between between previous register and current"', modify
label define error_messages 22 `"022 WARNING: Husband appears to have more than one wife in same register"', modify
label define error_messages 23 `"023 ERROR: Marital != 2, 3, but no wife found"', modify
label define error_messages 24 `"024 ERROR: Marital == 2, but wife found"', modify
label define error_messages 25 `"025 ERROR: Marital == 3, but live wife found"', modify
label define error_messages 26 `"026 ERROR: Marital != 2, 3, but located wife is dead (vital1 == 1)"', modify
label define error_messages 27 `"027 ERROR: Duplicated rhhead within a household"', modify
label define error_messages 28 `"028 ERROR: Invalid character with rhhead"', modify
label define error_messages 29 `"029 ERROR: Father appears to have changed since a previous register"', modify
label define error_messages 30 `"030 ERROR: Grandfather appears to have changed since a previous register"', modify
label define error_messages 31 `"031 ERROR: Appears older than a paternal ancestor in the same register"', modify
end

program define process_hukouce

local date_time "$S_DATE $S_TIME"
local date_time = subinstr("`date_time'",":","",.)
log using "../logs/Process_hukouce `date_time'", text replace

display "process_hukouce:starting"
*mk_consolidated_file test
mk_consolidated_file
link_individuals "`date_time'" 

error_check "`date_time'" 
marriages "`date_time'" 
ancestors "`date_time'" 
generated "`date_time'" 
kin_counts
generate_inventory
finalize_error_report "`date_time'" 
merged_error_report "`date_time'" 

display "process_hukouce:finished"
end

set mem 2g

process_hukouce
