## Instructions

### 1. Web Scraping

Web scraping is an interesting and popular topic nowadays. It is somewhat easy to get start but you will realize the complexity when you go deeper. The good news is that Python provides a bunch of packages and frameworks to relieve the stress, and it works perfectly in most of the scenarios. Ryan Mitchell's *["Web Scraping with Python"](http://shop.oreilly.com/product/0636920034391.do)* is a good reference book if you want to learn more.

For most of the web scraping tasks, it is always a good idea to understand the web structure in the first place.

#### About EDGAR

The main source where we will scrape the fund filing data is [EDGAR](https://www.sec.gov/edgar.shtml) (Electronic Data Gathering, Analysis, and Retrieval system). It says in the homepage that "All companies, foreign and domestic, are required to file registration statements, periodic reports, and other forms electronically through EDGAR". It also allows free access to its filings by corporations, funds, and individuals. 

EDGAR organizes its file paths and directory structure as follows. The index paths link to the raw ascii text version of the complete filing content, for example:

[/Archives/edgar/data/1122304/0001193125-15-118890.txt](https://www.sec.gov/Archives/edgar/data/1122304/0001193125-15-118890.txt)

In the example above, 

* ”/Archives/edgar/data/“ is the parent directory to all archived data. It is not browsable by us.
* "1122304" or "0001122304" is the <u>Central Index Key</u> (CIK) of the main entity of the submitted file. All the files about this entity are stored in the subdirectory "/Archives/edgar/data/1122304/".

* EDGAR called "0001193125-15-118890" as the <u>accession number</u>, a unique identifier assigned automatically to an accepted submission by EDGAR. 
  * The first set of numbers (0001193125) is the CIK of the entity submitting the filing.  The filer could be the company or a third party filer agent.
  * The next 2 numbers (15) represent the year. 
  * The last series of numbers (118890) represent a sequential count of submitted filings from that CIK. The count is usually, but not always, reset to 0 at the start of each calendar year.

CIK is a unique 10-digit identifier assigned by the EDGAR system to the filers when they sign up to make filings to the SEC. Some filer agents without a regulatory requirement to make disclosure filings with the SEC have a CIK but no searchable presence in the public EDGAR database. In the above example, the main entity and the filer entity is different and only the main entity CIK is searchable in the database. In some filings such as 10-Q and 10-K, the filer and the main entity is the same, namely, the listed company itself.

To sum up, the directory hierarchy is shown as follow

-- /Archives/edgar/data/

​	-- CIK/

​		-- Accession Number/

​			-- Document format files



#### Form N-PX

We then focus on the N-PX data of the funds. The official requirements says that "Form N-PX is to be used by a **registered management investment company** ... to file reports with the Commission, not later than August 31 of each year, containing the registrant’s **proxy voting record** for the most recent **twelve-month period ended June 30**, pursuant to section 30 of the Investment Company Act of 1940 and rule 30b1-4 thereunder."

There are no index keys for different forms. So we will use EDGAR's [boolean and advanced searching](https://www.sec.gov/cgi-bin/srch-edgar) tool to screen out the N-PX forms. Take Blackrock as an example, we can input

"n-px and company-name=blackrock"

in the search bar and click search. Then we get 215 documents about Blackrock's N-PX forms from 2018 to 2020. Now we can take a look at the querying URL:

https://www.sec.gov/cgi-bin/srch-edgar?text=n-px+and+company-name%3Dblackrock&first=2018&last=2020

When we implement automatic scraping, we only need to replace Blackrock with other fund family names and adjust the first and last parameters to get our searching URL as wish. After requesting this page, we can easily iterate through the pages and the tables to download the N-PX filings. Note that the fund family Blackrock has many different companies.



### 2. Data Extraction

SEC does not strictly restrict the format in which funds should report their N-PX forms, which makes everything a bit messy. Different fund families report N-PX in different ways. Even the same fund family will alter the format in different years. SEC is currently making progress to [structured disclosure](https://www.sec.gov/structureddata) with the assistance of inline XBRL. But for now, there is no perfect and elegant solution yet to this N-PX problem.

We stick to the Blackrock example to show how to automatically extract the voting data from these semi-structured txt files. We focus on one year here because within a year most fund families do not change the reporting format. 

First, we take a look at the hierarchy starting from the fund family:

-- Fund Family (e.g. Blackrock)

​	-- **Fund Company** (e.g. Blackrock Index Funds, Inc) *

​		-- Fund (e.g. iShares MSCI EAFE International Index Fund)

​			-- Voting Event

Each fund company will have a CIK in the EDGAR system and report one N-PX form each year. It means that each text file is at the level of fund company. A fund company can either have just one funds or several different funds. Therefore, there are two tiers left in each text file, i.e., funds and their respective voting events.

Second, we can separate the text into different funds and then different voting events.

Finally, we iterate through the hierarchy structure and output the data.

The implementation details are also shown in the Jupyter notebook.























