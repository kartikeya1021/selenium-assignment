import pymongo
import streamlit as st
from selenium import webdriver
from selenium.webdriver.common.keys import Keys
import time
from bs4 import BeautifulSoup
import re
import time

isJobRunning = False


def scrapingJob():
    global isJobRunning
    if not isJobRunning:
        return

    def scarper(city, id):

        WEBSITE_URL = f"https://99acres.com/search/property/buy/{city}-all?city={id}&preference=S&area_unit=1&res_com=R"
        chrome_options = webdriver.ChromeOptions()
        chrome_options.add_argument("--headless")
        driver = webdriver.Chrome(options=chrome_options)
        driver.get(WEBSITE_URL)
        time.sleep(5)
        html = driver.page_source
        soup = BeautifulSoup(html, 'html.parser')
        properties = soup.find_all('div', class_="projectTuple__descCont")

        property_map = {}
        for property in properties:

            property_name = property.find(
                'a', class_="projectTuple__projectName")
            property_price = property.find(
                'span', class_="configurationCards__configurationCardsHeading")
            property_details = property.find_all(
                "div", class_="configurationCards__cardContainer configurationCards__srpCardStyle")
            property_locality_ = property.find(
                "h2", class_="projectTuple__subHeadingWrap body_med ellipsis")
            property_locality = property_locality_.text
            property_locality = re.findall(r'in\s+([^,]+),', property_locality)
            if len(property_locality) > 0:
                property_locality = property_locality[0]
            else:
                property_locality = property_locality_.text

            property_area = []
            property_type = []
            property_price = []
            for property_detail in property_details:
                if property_detail:
                    property_type_ = property_detail.find(
                        "span", class_="list_header_semiBold configurationCards__configBandLabel")
                    if property_type_:
                        property_type.append(property_type_.text)

                    property_area_ = property_detail.find(
                        "span", class_="caption_subdued_medium configurationCards__cardAreaSubHeadingOne")
                    if property_area_:
                        property_area.append(property_area_.text)

                    property_price_ = property_detail.find(
                        "span", class_="list_header_semiBold configurationCards__cardPriceHeading")
                    if property_price_:
                        property_price.append(property_price_.text)

                    property_map[property_name.text] = {
                        "property_name": property_name.text,
                        "property_price": property_price,
                        "property_area": property_area,
                        "property_type": property_type,
                        "property_locality": property_locality,
                        "property_city": city,
                    }
                else:
                    continue
        driver.close()

        return property_map

    cities = ["Pune", "Delhi", "Mumbai", "Lucknow", "Agra",
              "Ahmedabad", "Kolkata", "Jaipur", "Chennai", "Bengaluru"]
    cities_map = {
        "Pune": 19,
        "Delhi": 1,
        "Mumbai": 12,
        "Lucknow": 205,
        "Agra": 197,
        "Ahmedabad": 45,
        "Kolkata": 25,
        "Jaipur": 177,
        "Chennai": 32,
        "Bengaluru": 20,
        "Hyderabad": 38,
    }
    city_wise_data = {}
    for city in cities:
        st.write(f"Scrapping {city} Data")
        placeholder = st.write("Please Wait....")

        city_wise_data[city] = scarper(city, cities_map[city])
        placeholder.empty()
        st.write(
            f"Scrapping {city} Data Completed got {len(city_wise_data[city])} properties")

    def saveInMongodbData(data):
        import pymongo
        myclient = pymongo.MongoClient("mongodb://localhost:27017/")
        mydb = myclient["99acres"]
        mycol = mydb["properties"]
        for city in data:
            for property in data[city]:
                mycol.insert_one(data[city][property])

    saveInMongodbData(city_wise_data)


myclient = pymongo.MongoClient("mongodb://localhost:27017/")
mydb = myclient["99acres"]
mycol = mydb["properties"]

st.title("99acres Data")
st.write("Data from 99acres")
st.write("Data is collected from 99acres.com using web scrapping")
st.write("Data is collected from 10 cities")

time1 = st.time_input('Set 1st time to run the scraping job')
time2 = st.time_input('Set 2nd time to run the scraping job')


if st.button("Run Scrapping Job Manually"):
    scrapingJob()
    if isJobRunning:
        st.write("Scrapping Job Running.....")
    else:
        if st.button("Stop Scrapping Job"):
            isJobRunning = False
            st.write("Scrapping Job Stopped")


if st.button("Run Scrapping Job Automatically"):
    import schedule
    import time

    def job():
        scrapingJob()
    schedule.every().day.at(str(time1)).do(job)
    schedule.every().day.at(str(time2)).do(job)
    if st.button("Stop Auto Scrapping Job", key="stop"):
        isJobRunning = False
    while True:
        schedule.run_pending()
        time.sleep(1)
        if not isJobRunning:
            break


if st.button("Show Data"):
    table = st.table(mycol.find({}))
