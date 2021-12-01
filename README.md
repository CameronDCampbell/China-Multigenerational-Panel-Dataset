# cmgpd
China Multigenerational Panel Dataset

The China Multigenerational Panel Datasets consist of household registers for Liaoning between 1749 and 1909 and Shuangcheng between 1866 and 1913 that we have entered, linked, and processed to create variables describing individuals, and their households, kin networks, and communities. [The CMGPD data are available for download at ICPPR](https://www.icpsr.umich.edu/web/ICPSR/series/265). This is repository of the code (STATA .do files) that read in the Excel files entered by coders to produce the STATA .dta files that make up the CMGPD.

Here they are in the order that they normally run:

[Read household registers as entered by coders in Excel](read_raw_ne_china_household_registers.do)

Reads in the Excel files as entered by the coders and produces corresponding .dta files, one for each register.

[Process household registers](process_ne_china_household_registers.do)

Combine the separate dta files for each register into a single file, link to produce personal identifiers, and create variables

[Produce CMGPD-Liaoning Extract for ICPSR](<CMGPD-LN Extract for ICPSR 7 cdc.do>)

Extract variables and rename them to produce the CMGPD-LN shared on ICPSR.

[Produce CMGPD-Shuangcheng Extract for ICPSR](<CMGPD_SC_extract_for_ICPSR 10 cdc.do>)

Extract variables and rename them to produce the CMGPD-SC shared on ICPSR.


