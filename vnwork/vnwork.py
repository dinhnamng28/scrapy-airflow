import requests
import json
import time
import csv
from datetime import datetime,timedelta
import os

def scrape_vietnamworks():
    url = "https://ms.vietnamworks.com/job-search/v1.0/search"
    page = 0

    payload = {
        "userId": 0,
        "query": "",
        "filter": [],
        "ranges": [],
        "order": [],
        "hitsPerPage": 50,
        "page": page,
        "retrieveFields": [
            "address",
            "benefits",
            "jobTitle",
            "salaryMax",
            "isSalaryVisible",
            "jobLevelVI",
            "isShowLogo",
            "salaryMin",
            "companyLogo",
            "userId",
            "jobLevel",
            "jobLevelId",
            "jobId",
            "jobUrl",
            "companyId",
            "approvedOn",
            "isAnonymous",
            "alias",
            "expiredOn",
            "industries",
            "workingLocations",
            "services",
            "companyName",
            "salary",
            "onlineOn",
            "simpleServices",
            "visibilityDisplay",
            "isShowLogoInSearch",
            "priorityOrder",
            "skills",
            "profilePublishedSiteMask",
            "jobDescription",
            "jobRequirement",
            "prettySalary",
            "requiredCoverLetter",
            "languageSelectedVI",
            "languageSelected",
            "languageSelectedId",
            "yearsOfExperience"
        ]
    }

    headers = {
        'Connection': 'keep-alive',
        'Origin': 'https://www.vietnamworks.com',
        'Referer': 'https://www.vietnamworks.com/',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-site',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/130.0.0.0 Safari/537.36 Edg/130.0.0.0',
        'accept': '*/*',
        'accept-language': 'vi',
        'content-type': 'application/json',
        'sec-ch-ua': '"Not)A;Brand";v="99", "Microsoft Edge";v="127", "Chromium";v="127"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        'x-source': 'Page-Container'
    }

    jobs_list = []
    while page < 160:
        response = requests.request("POST", url, headers=headers, data=json.dumps(payload))
        response_data = response.json()

        time.sleep(3)

        for job in response_data.get('data', []):
            job_url = job.get('jobUrl', 'N/A')

            title = job.get('jobTitle')
            groupJobFunctionsV3 = job.get('groupJobFunctionsV3',[])
            group_job_name = groupJobFunctionsV3['groupJobFunctionV3NameVI']

            jobFunctionsV3 = job.get('jobFunctionsV3',[])
            job_type = jobFunctionsV3['jobFunctionV3NameVI']

            job_level = job.get('jobLevelVI', 'Khác')

            time_update = job.get('approvedOn', 'N/A')
            time_expire = job.get('expiredOn', 'N/A')

            if time_update:
                time_update = datetime.strptime(time_update[:10], "%Y-%m-%d").date()

            if time_expire:
                time_expire = datetime.strptime(time_expire[:10], "%Y-%m-%d").date()

            last_week_date = datetime.today().date() - timedelta(weeks=1)

            if time_update and time_update < last_week_date:
                continue

            pretty_salary = job.get('salary', '0')
            try:
                if pretty_salary:
                    pretty_salary = float(pretty_salary)
                    if pretty_salary > 1000000:
                        pretty_salary = pretty_salary / 25000
                else:
                    pretty_salary = 0
            except ValueError:
                pretty_salary = 0

            city_func = job.get('workingLocations', [])
            city = city_func[0].get('cityName', 'Khac') if city_func else 'Khac'
            address = city_func[0].get('address', 'Not Show') if city_func else 'Not Show'

            company = job.get('companyName')
            company_url = job.get('companyLogo', 'Not Show')

            job_description = job.get('jobDescription', 'N/A').strip().replace('\n', ' ').replace('\r', ' ')
            job_requirement = job.get('jobRequirement', 'N/A').strip().replace('\n', ' ').replace('\r', ' ')

            years_of_exp = job.get('yearsOfExperience', 0)
            try:
                if years_of_exp:
                    years_of_exp = float(years_of_exp)
            except ValueError:
                years_of_exp = 0

            benefit_info = job.get('benefits', [])
            benefit_names = ','.join(benefit['benefitName'] for benefit in benefit_info) if benefit_info else 'N/A'

            job_data = {
                'address': address,
                'benefit': benefit_names,
                'city': city,
                'company_name': company,
                'company_url': company_url,
                'exp': years_of_exp,
                'group_job': group_job_name,
                'job_des': job_description,
                'job_level': job_level,
                'job_req': job_requirement,
                'job_type': job_type,
                'salary' : pretty_salary,
                'time_expire': time_expire,
                'time_update': time_update,
                'title': title,
                'url': job_url,
                'web': "VietNamWork"
            }

            jobs_list.append(job_data)

        page += 1
        payload['page'] = page
    
    return jobs_list

# Hàm lưu dữ liệu vào file CSV
def save_data_to_csv(jobs_list, file_path='/opt/airflow/data/vnwork.csv'):
    # Kiểm tra nếu dữ liệu có sẵn
    if not jobs_list:
        print("No data to save.")
        return

    # Lưu dữ liệu vào file CSV
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    with open(file_path, 'w', encoding='utf-8', newline='') as f:
        fieldnames = jobs_list[0].keys() if jobs_list else []
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(jobs_list)
