
* Creates version of the extract of CMGPD-LN for ICPSR

* data_directory refers to directory containing 'frozen' version to ensure all extracts are consistent


local data_directory "combined ICPSR master"
local extract_directory "CMGPD-LN Extracts"

set more off
set mem 2g

tempfile liaoning scp liaoning_digital father_name gf_name mother_name wife wife_name hh_head_name husband alive_wives alive_husbands married_moms widowed_moms

use id date kin_md_f_1_ma kin_hh_md_f_1_ma using "../`data_directory'/household_registers_kin_counts_married" 
sort date id
by date id: keep if _n == 1
sort date id
save "`married_moms'"

use id date kin_wid_f_1_ma kin_hh_wid_f_1_ma using "../`data_directory'/household_registers_kin_counts_widowed" 
sort date id
by date id: keep if _n == 1
sort date id
save "`widowed_moms'"

use id date present sex using "../`data_directory'/household_registers_generated" if sex == 1 & present
sort id date
by id date: keep if _n == 1
rename id w_id
sort w_id date
drop sex
save "`alive_wives'"

use id date present sex using "../`data_directory'/household_registers_generated" if sex == 2 & present
sort id date
by id date: keep if _n == 1
rename id h_id
sort h_id date
save "`alive_husbands'"

use "Hukouce"
sort dataset
save, replace

use "../`data_directory'/household_registers_raw" if dataset < 100
keep id date name
sort id date
by id: keep if _n == _N
rename id f_id_1 
rename name f_name
drop date
sort f_id_1 
save "`father_name'"

rename f_id_1 f_id_2
sort f_id_2
rename f_name gf_name
save "`gf_name'"

replace gf_name = subinstr(gf_name," shi","",.)

rename f_id_2 m_id_1
sort m_id_1
rename gf_name m_name
save "`mother_name'"

rename m_id_1 w_id
sort w_id
rename m_name wife_name
save "`wife_name'"

use h_id w_id date marriage_first dataset using "../`data_directory'/household_registers_marriages" if dataset < 100

sort h_id date w_id
by h_id date w_id: keep if _n == 1

sort w_id date
merge w_id date using "`alive_wives'", nokeep
keep if present != .
drop present _merge

sort h_id date marriage_first w_id
by h_id date: generate j = _n 
drop marriage_first dataset

reshape wide w_id, i(h_id date) j(j)

rename h_id id

generate year = date+1000
drop date

replace year = 1909 if year == 1910

sort id year
save "`wife'"

use h_id w_id date marriage_first dataset using "../`data_directory'/household_registers_marriages" if dataset < 100

sort w_id date h_id
by w_id date h_id: keep if _n == 1

sort h_id date
merge h_id date using "`alive_husbands'", nokeep
keep if present != .
drop present _merge

sort w_id h_id 
by w_id h_id: generate h_obs = _N
sort w_id date h_obs
by w_id date: keep if _n ==  _N

drop marriage_first dataset h_obs

rename w_id id

generate year = date+1000
drop date

replace year = 1909 if year == 1910

sort id year
save "`husband'"

use dataset date case interpol id age clan hid recno vital1 vital2 marital gen rhhead sex name occu disabil birthyr month day hour using "../`data_directory'/household_registers_raw" if dataset < 100

sort dataset recno
generate record_number = string(_n, "%09.0f")

desc, short

desc, short 
sort date id
merge date id using "`married_moms'", nokeep
drop _merge

sort date id
merge date id using "`widowed_moms'", nokeep
drop _merge

* The mother_alive had to be constructed using different variables based on presence of maternal (*_ma) kin

generate byte mother_alive = kin_wid_f_1_ma | kin_md_f_1_ma
generate byte kin_f_1_ma = mother_alive
generate byte kin_hh_f_1_ma = kin_hh_wid_f_1_ma | kin_hh_md_f_1_ma

desc, short

replace date = 909 if date == 910 & dataset < 100
generate int year = date+1000

rename clan zu_id
rename hid hh_id
rename gen generation
rename birthyr bazi_year
rename month bazi_month
rename day bazi_day
rename hour bazi_hour

replace bazi_year = -99 if bazi_year == . | bazi_year <= 0 | bazi_year > 12
replace bazi_month = -99 if bazi_month == . | bazi_month <= 0 | bazi_month > 12
replace bazi_day = -99 if bazi_day == . | bazi_day <= 0 | bazi_day > 30
replace bazi_hour = -99 if bazi_hour == . | bazi_hour <= 0 | bazi_hour > 12

sort id 
merge id using "../`data_directory'/household_registers_ancestors", keep(f_id_1 f_id_2 f_id_3 f_id_4 m_id_1 m_id_2 m_id_3 m_id_4) nokeep
drop _merge

generate birthyear = year + 1 - age

sort id year
merge id year using "`wife'", nokeep
drop _merge

sort id year
merge id year using "`husband'", nokeep
drop _merge

compress

local kin_merge ""

/*
foreach x in varlist kin_m_1-kin_m_1_pa kin_hh_m_1-kin_hh_m_1_pa {
	local kinmerge "`kin_merge' `x'"
}
*/

desc, short
merge dataset recno using "../`data_directory'/household_registers_kin_counts_all.dta", sort nokeep 
drop _merge

merge id date recno using "../`data_directory'/household_registers_kin_counts_widowed.dta", sort nokeep
drop _merge

merge id date recno using "../`data_directory'/household_registers_kin_counts_married.dta", sort nokeep
drop _merge

desc, short

/*
sort dataset recno
by dataset recno: drop if _n > 1
*/

generate byte brother_count = kin_hh_m_1 
generate byte sister_count = kin_hh_f_1
generate byte male_cousin_count = kin_hh_m_2
generate byte female_cousin_count = kin_hh_f_2
generate byte uncle_count = kin_hh_m_2_pa
generate byte aunt_count = kin_hh_f_2_pa
generate byte father_alive = kin_hh_m_1_pa

merge dataset recno using "../`data_directory'/household_registers_generated.dta", sort nokeep keep(unique_village address present surname_yihu at_risk_die at_risk_marry at_risk_remarry next_die next_tao next_boys next_girls boys girls next_marry next_remarry present next3 next6 position tao tui lao area location artisan soldier unique_village valid_village zu_zhang occu bai_zong qian_zong xiao_qi_xiao bi_tie_shi guan_xue_sheng ling_cui zhi_shi_ren service has_surname non_han_name diminutive rustic number pin dingdai juan hou_bu wei_guan man_zhou disabled income honorif man_zhou new purchased tou_cong expelled gao_li bao_yang birth_order birth_order_sex seniority seniority_sex ever_tao exam gao_li founder_ancestor founder_inferred gu hh_divide_next hh_size popstatus qi unique_group unique_hh_id unique_yihu)

drop _merge

generate mother_id = m_id_1
generate father_id = f_id_1
generate grandfather_id = f_id_2

rename id person_id
rename w_id1 wife_1_id
rename w_id2 wife_2_id
rename next3 next_3
rename next6 next_6
rename tao absconded
rename tui retired
rename lao old
rename boys son_count
rename girls daughter_count
rename next_tao next_absconded

* Renames for analytic release

*rename bao_yang adopted
rename diminutive diminutive_name
rename dingdai ding_dai
rename disabil disability_code
rename ever_tao ever_absconded
rename exam examination
rename founder_ancestor founder_id
rename founder_inferred founder_inferred_id
rename gu dead
rename honorif honorific
rename income estimated_income
rename new new_ding
rename number number_name
rename occu position_code
rename pin rank
rename popstatus population_category
rename purchased purchased_title
rename qi banner
rename rustic rustic_name
rename service service_ding
rename unique_yihu unique_yi_hu

replace unique_village = . if !valid_village
drop valid_village

rename h_id husband_id

sort dataset year case interpol

rename rhhead relationship

generate household_id = string(dataset,"%03.0f")+string(date+1000,"%04.0f")+string(zu_id,"%03.0f")+string(hh_id,"%02.0f")
*generate record_id = string(dataset,"%03.0f")+string(recno,"%06.0f")
generate register_seq = string(case,"%05.0f")+string(interpol,"%02.0f")
generate record_id = string(dataset,"%03.0f")+string(year,"%04.0f")+register_seq

mvencode founder_inferred_id father_id grandfather_id husband_id wife_1_id wife_2_id founder_id unique_group unique_yi_hu unique_hh_id, mv(-99) override 

sort person_id
generate max_id = person_id[_N]

generate byte father_id_imputed = father_id > max_id
generate byte grandfather_id_imputed = grandfather_id > max_id

drop max_id

* Kinship linking variables
* Fixed August 6, 2011

tostring founder_id, replace format("%08.0f")
tostring person_id, replace format("%08.0f")
tostring mother_id, replace format("%08.0f")
tostring father_id, replace format("%08.0f")
tostring grandfather_id, replace format("%08.0f")
tostring husband_id, replace format("%08.0f")
tostring wife_1_id, replace format("%08.0f")
tostring wife_2_id, replace format("%08.0f")
tostring f_id_* m_id_*, replace format("%08.0f")

* Grouping variables

tostring founder_inferred_id, replace format("%010.0f")
tostring unique_group, replace format("%010.0f")
tostring unique_yi_hu, replace format("%010.0f")
tostring unique_hh_id, replace format("%010.0f")

* Want the strings for missing to be -99 instead of -000000099 or whatever

foreach x of varlist f_id_1 f_id_2 f_id_3 f_id_4 m_id_1 m_id_2 m_id_3 m_id_4 founder_inferred_id father_id grandfather_id husband_id wife_1_id wife_2_id founder_id unique_group unique_yi_hu unique_hh_id {
	replace `x' = "-99" if real(`x') == -99 | real(`x') == 0
	replace `x' = "-98" if real(`x') == -98
}

* Force spouse IDs to structural missing for people who are widowed or single

replace wife_1_id = "-98" if sex == 1 | marital == 2 | marital == 3
replace wife_2_id = "-98" if sex == 1 | marital == 2 | marital == 3
replace husband_id = "-98" if sex == 2 | marital == 2 | marital == 3

* Force birth order, seniority variables to missing for individuals without a father ID
* Added 4/12/2011

foreach x of varlist birth_order birth_order_sex seniority seniority_sex brother_count sister_count father_alive {
	replace `x' = -98 if real(father_id) == -99
}

* Force household size, birth order, senior variables to missing where their computed value is zero. This is mainly married/widowed women, and a small number of men.
* added 4/24/2014

foreach x of varlist hh_size birth_order birth_order_sex seniority seniority_sex {
	replace `x' = -98 if `x' == 0
}

replace mother_alive = -98 if real(mother_id) == -99

* Force all kinship counts to structural missing when relevant common ancestor isn't identified 

foreach x of varlist kin*1 {
	replace `x' = -98 if real(f_id_1) == -99 | real(f_id_1) == -98
}

foreach x of varlist kin*2 kin*2_pa {
	replace `x' = -98 if real(f_id_2) == -99 | real(f_id_2) == -98
}

foreach x of varlist kin*3 kin*3_pa {
	replace `x' = -98 if real(f_id_3) == -99 | real(f_id_3) == -98
}

foreach x of varlist kin*4 kin*4_pa {
	replace `x' = -98 if real(f_id_4) == -99 | real(f_id_4) == -98
}

replace kin_m_1_pa = -98 if real(f_id_1) == -99 | real(m_id_1) == -98
replace kin_f_1_ma = -98 if real(m_id_1) == -99 | real(m_id_1) == -98
replace kin_hh_f_1_ma = -98 if real(m_id_1) == -99 | real(m_id_1) == -98

replace marital = 4 if marital > 4
rename marital marital_status
replace marital_status = -99 if marital_status == 0

* There are no zero ages in the original data, if an age is zero, it means it was missing in the original data.  If it is less than zero, 
* some other kind of problem, probably age originally missing and at some point we made a failed effort at machine assignment of age.

replace age = -99 if age <= 0

local date_time "$S_DATE $S_TIME"
local date_time = subinstr("`date_time'",":","",.)

rename hh_id household_seq
rename zu_id zu_seq

drop recno
*rename recno record_seq

*label variable record_seq "Record sequence number, within dataset"
label variable zu_seq "Zu sequence number, within register"
label variable household_seq "Household sequence number, within zu"

*label variable record_id "Unique record identifier, concatenation of dataset and record_seq"

generate died = vital1 == 1 | vital2 == 1
generate married_out = vital1 == 2 | vital2 == 2
generate remarried_out = vital1 == 3 | vital2 == 3
generate adopted_in = vital1 == 8 | vital2 == 8
generate adopted_out = vital1 == 11 | vital2 == 11

rename area prefecture
rename location region

drop date case interpol
rename age age_in_sui
rename vital1 event_1
rename vital2 event_2

replace event_1 = -99 if event_1 == 0 | event_1 > 25
replace event_2 = -99 if event_2 == 0 | event_2 > 25

generate byte no_status = (position_code == 0 | position_code == -1 | position_code == .)
replace no_status = -98 if sex != 2

replace household_seq = -98 if year < 1789
replace hh_size = -98 if year < 1789
replace unique_hh_id = "-98" if year < 1789
replace hh_divide_next = -98 if year < 1789
* If banner is 0, it means that there was a village recorded, but no banner recorded alongside the village
replace banner = -99 if banner == 0
* If banner is system missing, it means that there wasn't any address recorded at all
replace banner = -98 if banner == .

sort record_id

order record_number record_id person_id surname_yihu name mother_id father_id father_id_imputed grandfather_id grandfather_id_imputed wife_1_id wife_2_id husband_id f_id_1-f_id_4 m_id_1-m_id_4 household_id unique_hh_id unique_group unique_yi_hu founder_inferred_id founder_id dataset year register_seq zu_seq household_seq region prefecture banner population_category unique_village address relationship sex generation marital_status age_in_sui birthyear brother_count sister_count male_cousin_count female_cousin_count uncle_count aunt_count father_alive mother_alive son_count daughter_count present died married_out remarried_out next_die next_marry next_remarry next_absconded next_boys next_girls next_3 next_6 at_risk_die at_risk_marry at_risk_remarry birth_order birth_order_sex seniority seniority_sex hh_size hh_divide_next position_code absconded ever_absconded expelled tou_cong adopted_in adopted_out dead no_status retired old new_ding service_ding disabled disability_code position rank ding_dai zu_zhang artisan soldier bai_zong qian_zong xiao_qi_xiao bi_tie_shi guan_xue_sheng zhi_shi_ren ling_cui honorific examination juan purchased_title has_surname non_han_name diminutive_name rustic_name number_name man_zhou gao_li kin_m_1-kin_m_1_pa kin_f_1_ma kin_hh_m_1-kin_hh_m_1_pa kin_hh_f_1_ma kin_md_m_1-kin_md_m_1_pa kin_md_f_1_ma kin_hh_md_m_1-kin_hh_md_m_1_pa kin_hh_md_f_1_ma kin_wid_m_1-kin_wid_m_1_pa kin_wid_f_1_ma kin_hh_wid_m_1-kin_hh_wid_m_1_pa kin_hh_wid_f_1_ma bazi_year bazi_month bazi_day bazi_hour

mvencode unique_village age_in_sui birthyear prefecture, mv(-99) override

replace birthyear = -98 if age_in_sui == -99

* Force constructed counts, flags etc. for people who are not present (dead, married out etc.) to structural missing, -98

foreach x of varlist brother_count-daughter_count next_die-at_risk_remarry {
	replace `x' = -98 if !present
} 

* Force position/title/status variables to structural missing for women
* Leaving position_code as in original since sometimes it is non-zero for women, but changing 0 and -1 to -99

replace position_code = -99 if position_code == 0 | position_code == -1

replace disability_code = -99 if disability_code == 0

foreach x of varlist absconded-gao_li estimated_income {
	replace `x' = -98 if sex != 2
}

foreach x of varlist kin_hh_m_1-kin_hh_f_1_ma {
	replace `x' = -98 if year < 1789
}

* Force variables recording occurrance of events in next register to structural missing in the last register in each series

bysort dataset (year): generate byte last_register = year == year[_N]

foreach x of varlist hh_divide_next next_die next_marry next_remarry next_absconded next_boys next_girls {
	replace `x' = -98 if last_register
}

replace sex = -99 if sex == 0

label define marital 1 "Married" 2 "Unmarried" 3 "Widowed" 4 "Remarried"
label values marital_status marital

label define sex1 1 "Female" 2 "Male"
label values sex sex1 

compress

preserve

keep record_number person_id mother_id father_id father_id_imputed grandfather_id grandfather_id_imputed wife_1_id wife_2_id husband_id household_id dataset year register_seq zu_seq household_seq region prefecture unique_village relationship sex generation marital_status age_in_sui birthyear brother_count sister_count male_cousin_count female_cousin_count uncle_count aunt_count father_alive mother_alive son_count daughter_count present died married_out remarried_out next_die next_marry next_remarry next_absconded next_boys next_girls next_3 next_6 at_risk_die at_risk_marry at_risk_remarry absconded no_status retired old position zu_zhang artisan soldier bazi_year bazi_month bazi_day bazi_hour

rename unique_village unique_village_id

outsheet using "../`extract_directory'/LN ICPSR basic `date_time'.txt", replace nolabel 
save "../`extract_directory'/LN ICPSR basic `date_time'", replace

restore
preserve

keep record_number dataset address name surname_yihu

sort dataset address

merge dataset address using "Village master", nokeep keep(assigned_name lati longi suoshu) 

replace lati = -999 if lati == . | lati == 0
replace longi = -999 if longi == . | longi == 0
replace suoshu = -99 if suoshu == .
replace address = -99 if address == 0

drop _merge 

rename address raw_village_id
rename lati latitude
rename longi longitude

outsheet using "../`extract_directory'/LN ICPSR restricted `date_time'.txt", replace nolabel 
save "../`extract_directory'/LN ICPSR restricted `date_time'", replace

restore
preserve

keep adopted_in adopted_out bai_zong banner adopted_in adopted_out birth_order birth_order_sex bi_tie_shi diminutive_name ding_dai disability_code disabled estimated_income event_1 event_2 ever_absconded examination expelled founder_id founder_inferred_id gao_li dead guan_xue_sheng has_surname hh_size hh_divide_next honorific hou_bu juan man_zhou non_han_name ling_cui man_zhou new_ding number_name rank population_category position_code purchased_title qian_zong record_number rustic_name seniority seniority_sex service_ding tou_cong unique_group unique_hh_id unique_yi_hu xiao_qi_xiao zhi_shi_ren 

order adopted_in adopted_out bai_zong banner adopted_in adopted_out birth_order birth_order_sex bi_tie_shi diminutive_name ding_dai disability_code disabled estimated_income event_1 event_2 ever_absconded examination expelled founder_id founder_inferred_id gao_li dead guan_xue_sheng has_surname hh_size hh_divide_next honorific hou_bu juan man_zhou non_han_name ling_cui man_zhou new_ding number_name rank population_category position_code purchased_title qian_zong record_number rustic_name seniority seniority_sex service_ding tou_cong unique_group unique_hh_id unique_yi_hu xiao_qi_xiao zhi_shi_ren 

* Juan originally had a lot of distinct codes according to what it prefixed.  We're collapsing it to a flag variable.
* Leave it in after all

* replace juan = 1 if juan > 1

label define titles 1 "bi tie shi" 2 "bi zheng" 3 "cui zhang" 4 "fang yu" 5 "guan xue sheng" 6 "jian sheng" 7 "jiao guan" 8 "jiao xi" 9 "jing sheng" 10 "ku shi" 11 "ling cui" 12 "mu zhang" 13 "pi jia" 14 "qian feng xiao" 15 "tai ling" 16 "xiao qi xiao" 17 "xie ling" 18 "xun dao" 19 "yun qi wei" 20 "zhang jing" 21 "zhi fu" 22 "zhi shi ren" 23 "zhu shi" 24 "zuo ling" 25 "fu du tong" 26 "jian fang fu du tong" 27 "zhi xian" 28 "tang zhu shi" 29 "xiao zheng"
label define titles 30 "gong" 31 "ying qian zong" 32 "fu yuan cheng xian" 33 "yi xu" 34 "jiu pin", add
label define titles 35 "ba pin" 36 "qi pin" 37 "liu pin" 38 "wu pin" 39 "si pin", add
label define titles 40 "san pin" 41 "er pin" 42 "yi pin" 43 "fu jing li xian" 44 "gong pai", add
label define titles 45 "gong sheng" 46 "hong mu" 47 "hou bu bie tie shi" 48 "hou bu cui zhang" 49 "hou bu ku shi", add
label define titles 50 "jing li zhi xian" 51 "kong ri" 52 "liu pin bi gong" 53 "xun cha" 54 "yi sheng" 55 "zan li lang xian" 56 "zhou tong xian" 57 "shou yu suo" 58 "tong pan" 59 "fu qian zong", add
label define titles 60 "si ku", add

label values hou_bu titles
label values juan titles

outsheet using "../`extract_directory'/LN ICPSR analytic `date_time'.txt", replace nolabel 
save "../`extract_directory'/LN ICPSR analytic `date_time'", replace

restore

keep record_number f_id_1-f_id_4 m_id_1-m_id_4 kin_m_1-kin_m_1_pa kin_f_1_ma kin_hh_m_1-kin_hh_m_1_pa kin_hh_f_1_ma kin_md_m_1-kin_md_m_1_pa kin_md_f_1_ma kin_hh_md_m_1-kin_hh_md_m_1_pa kin_hh_md_f_1_ma kin_wid_m_1-kin_wid_m_1_pa kin_wid_f_1_ma kin_hh_wid_m_1-kin_hh_wid_m_1_pa kin_hh_wid_f_1_ma

outsheet using "../`extract_directory'/LN ICPSR kinship `date_time'.txt", replace nolabel 
save "../`extract_directory'/LN ICPSR kinship `date_time'", replace

drop _all

use "Disability master" if dataset <= 29 & disabil > 0
keep dataset disabil disease 
rename disabil disability_code
rename disease condition_pinyin

sort dataset disability_code 

outsheet using "../`extract_directory'/LN ICPSR disability `date_time'.txt", replace nolabel 
save "../`extract_directory'/LN ICPSR disability `date_time'", replace

drop _all

use "Occupation master" if dataset <= 29 & occu >= 1
keep dataset occu occuname core_occu

rename occu position_code
rename occuname position_pinyin
rename core_occu position_core

sort dataset position_code
 
outsheet using "../`extract_directory'/LN ICPSR position `date_time'.txt", replace nolabel 
save "../`extract_directory'/LN ICPSR position `date_time'", replace

drop _all



