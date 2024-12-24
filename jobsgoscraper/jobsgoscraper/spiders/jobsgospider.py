import scrapy
from jobsgoscraper.items import JobsgoscraperItem
from datetime import datetime, timedelta


class JobsgospiderSpider(scrapy.Spider):
    name = "jobsgospider"
    #allowed_domains = ["jobsgo.vn"]
    start_urls = ["https://jobsgo.vn/viec-lam.html"]

    def parse(self, response):
        jobs = response.xpath("//div[contains(@class, 'item-click')]")

        for job in jobs:
            job_url = job.xpath(".//div[@class='brows-job-position']/h3/a/@href").get()

            if job_url:
                yield response.follow(job_url, callback=self.parse_job_page)
        
        next_page_url = response.xpath("//li[@class='next']/a/@href").get()
        if next_page_url:
            if "https://jobsgo.vn" not in next_page_url:
                next_page_url = "https://jobsgo.vn" + next_page_url

            yield response.follow(next_page_url,callback=self.parse)
        
    def parse_job_page(self, response):
        job_item = JobsgoscraperItem()

        url = response.url
        title = response.xpath("//h1[@class='media-heading text-semibold']/text()").get()
        company_name = response.xpath("//div[@class='media-body']/h2/a/text()").get()
        
        company_url = response.xpath("//div[@class='media-body']/h2/a/@href").get()
        
        time_update = response.xpath("//p[contains(text(), 'Ngày đăng tuyển')]/following-sibling::p[1]/text()").get().strip()
        time_expire = response.xpath("//span[@class='deadline']/text()").get()

        # Kiểm tra và xử lý giá trị của time_expire
        if time_expire:
            try:
                time_expire = int(time_expire)  
            except ValueError:
                time_expire = 0  
        else:
            time_expire = 0  

        try:
            time_update = datetime.strptime(time_update, "%d/%m/%Y").date()  
        except ValueError:
            time_update = datetime.now() 


        one_week_ago = datetime.now().date() - timedelta(weeks=1)
        if time_update < one_week_ago:
            return
        
        # Tính toán thời gian hết hạn
        time_expire = time_update + timedelta(days=time_expire)
        
        salary = response.xpath("//span[@class='saraly']/text()").get().strip()
        try:
            if salary:
                if 'Thỏa thuận' in salary:
                    salary = 0
                elif 'Đến' in salary:
                    salary = salary.replace("triệu VNĐ","").replace("Đến","").strip()
                    salary = int(salary)*1000000/25000
                else:
                    salary = salary.replace("triệu VNĐ","").replace(" ","")
                    min_salary, max_salary = salary.split('-')
                    min_salary = int(min_salary) *1000000/ 25000  
                    max_salary = int(max_salary) *1000000/ 25000  
                    salary = (min_salary+ max_salary)/2  
            else:
                salary = 0
        except:
            salary = 0

        exp = response.xpath("//p[contains(text(), 'Yêu cầu kinh nghiệm')]/following-sibling::p[1]/text()").get()
        try:
            if exp is not None:  # Kiểm tra nếu exp không phải là None
                if '-' in exp:
                    exp = exp.replace(" ", "").split('-')[0]
                elif 'Dưới' in exp:
                    exp = exp.replace("Dưới", "").replace("năm", "").strip()  
                elif 'Trên' in exp:
                    exp = exp.replace("Trên", "").replace("năm", "").strip() 
                elif "Không yêu cầu" in exp:
                    exp = 0
                else:
                    exp = 0 
            else:
                exp = 0
        except Exception as e:  
            exp = 0
        
        job_level = response.xpath("//p[contains(text(), 'Vị trí/chức vụ')]/following-sibling::p[1]/text()").get()
        if job_level:
            job_level = job_level.strip()
        else:
            job_level = 'Nhân Viên'

        group_job = response.xpath("//p[contains(text(), 'Ngành nghề')]/following-sibling::div[@class='list']/a[1]/text()").get()

        if group_job:
            group_job = group_job.strip()
        else:
            group_job = "Khac"

        job_type = response.xpath("//p[contains(text(), 'Ngành nghề')]/following-sibling::div[@class='list']//a/text()").getall()
        if job_type:
            job_type = ', '.join(job.strip() for job in job_type)  # Nối các giá trị lại với nhau bằng dấu phẩy và loại bỏ khoảng trắng
        else:
            job_type = "Khac"  # Nếu không có giá trị nào, gán là "Not Show"

        benefits = response.xpath("//h2[contains(text(), 'Quyền lợi được hưởng')]/following-sibling::div/ul/li/text()").getall()
        benefits_cleaned = [benefit.strip() for benefit in benefits]
        benefit = '. '.join(benefits_cleaned)

        job_description = response.xpath("//h2[contains(text(), 'Mô tả công việc')]/following-sibling::div[1]//text()").getall()
        job_description = [desc.strip() for desc in job_description if desc.strip()]
        job_des = ', '.join(job_description)

        job_requirements = response.xpath("//h2[contains(text(), 'Yêu cầu công việc')]/following-sibling::div[1]//text()").getall()
        job_requirement = [desc.strip() for desc in job_requirements if desc.strip()]
        job_req = ', '.join(job_requirement)

        city = response.xpath("//div[@class='data giaphv']//span[contains(text(), 'Việc làm')]/text()").get()
        if city:
            city = city.replace('Việc làm ', '').strip()

        address = response.xpath("//div[@class='data giaphv']/p/text()").get()
        if address:
            address = ' '.join(address.split()).strip()
        else:
            address = 'Not Show'
        
        job_item['url'] = url
        job_item['title'] = title
        job_item['company_name'] = company_name
        job_item['company_url'] = company_url
        job_item['time_update'] = time_update
        job_item['time_expire'] = time_expire
        job_item['salary'] = salary
        job_item['exp'] = exp
        job_item['job_level'] = job_level
        job_item['group_job'] = group_job
        job_item['job_type'] = job_type
        job_item['benefit'] = benefit
        job_item['job_des'] = job_des
        job_item['job_req'] = job_req
        job_item['city'] = city
        job_item['address'] = address
        job_item['web'] = 'Jobsgo'

        yield job_item

