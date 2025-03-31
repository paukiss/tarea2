# Define your item pipelines here
#
# Don't forget to add your pipeline to the ITEM_PIPELINES setting
# See: https://docs.scrapy.org/en/latest/topics/item-pipeline.html


# useful for handling different item types with a single interface
# Useful for handling different item types with a single interface
from itemadapter import ItemAdapter
from datetime import datetime
from dateutil import parser
import emoji
import re
from urllib.parse import urlparse
import os
import psycopg2
from dotenv import load_dotenv


class NewspaperCollectorPipeline:

    def __init__(self) -> None:
        # Información de conexión con la base de datos
        hostname = os.getenv('DB_HOST')
        username = os.getenv('DB_USER')
        password = os.getenv('DB_PASSWORD')
        database = os.getenv('DB_DATABASE')

        self.connection = psycopg2.connect(
            host=hostname,
            user=username,
            password=password,
            dbname=database
        )

        self.cur = self.connection.cursor()

        self.cur.execute("""
            CREATE TABLE IF NOT EXISTS job_data (
                id SERIAL PRIMARY KEY,
                url TEXT,
                title TEXT,
                company TEXT,
                location TEXT,
                type_job TEXT,
                job_description TEXT,
                date_published TEXT,
                date_expiration TEXT,
                date_saved_iso TEXT
            );
        """)

        self.connection.commit()


    def process_item(self, item, spider):
        # Convert the item to a dictionary first
        transformed = dict(item)

        # Convert all fields to lowercase
        for key, value in transformed.items():
            if transformed[key] and isinstance(transformed[key], str):
                transformed[key] = transformed[key].lower()

        # Convert empty strings to None
        for field in transformed:
            if transformed[field] == "" or transformed[field] == " " or transformed[field] == "null":
                transformed[field] = None

        # Process job description - remove emojis and links
        if 'job_description' in transformed and transformed['job_description']:
            if isinstance(transformed['job_description'], list):
                cleaned_desc = []
                for paragraph in transformed['job_description']:
                    # Remove emojis and keep only alphanumeric
                    paragraph = self.clean_text(paragraph)
                    # Remove links
                    paragraph = self.remove_links(paragraph)
                    cleaned_desc.append(paragraph)
                transformed['job_description'] = ' '.join(cleaned_desc)
            elif isinstance(transformed['job_description'], str):
                # Remove emojis and keep only alphanumeric
                transformed['job_description'] = self.clean_text(transformed['job_description'])
                # Remove links
                transformed['job_description'] = self.remove_links(transformed['job_description'])

        # Convert dates to datetime
        self.convert_date_fields(transformed)

        # Format Timestamp
        if 'date_saved' in transformed:
            try:
                dt = datetime.fromisoformat(transformed['date_saved'])
                transformed['date_saved_iso'] = dt.isoformat()
            except:
                pass


        if 'url' in transformed and transformed['url']:
            transformed['domain'] = self.extract_domain(transformed['url'])

        # Determine if job is active or expired
        if 'date_expiration' in transformed and transformed['date_expiration']:
            transformed['status'] = self.get_job_status(transformed['date_expiration'])

        self.cur.execute("""
            SELECT * FROM job_data
            WHERE url = %s
        """, (transformed['url'],))

        # transformed['status'] = transformed['date_expiration'].apply(lambda x:self.get_job_status())

        res = self.cur.fetchone()

        if res:
            print(f"THIS item: {transformed['url']} is already in the DB.")
            raise Exception(f"The item is already in the DB.")
        else:
            self.cur.execute("""
                INSERT INTO job_data (
                    url, title, company, location, type_job,
                    job_description, date_published, date_expiration, date_saved_iso
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """, (
                transformed['url'],
                transformed['title'],
                transformed['company'],
                transformed['location'],
                transformed['type_job'],
                transformed['job_description'],
                transformed['date_published'],
                transformed['date_expiration'],
                transformed['date_saved_iso']
            ))

            self.connection.commit()

        return transformed


        def close_connection(self, spider):
            self.cur.close()
            self.connection.close()


        def clean_text(self, text):
            """Remove all non-alphanumeric characters from text"""
            if not text:
                return text
            # Keep only alphanumeric characters (a-z, A-Z, 0-9)
            return re.sub(r'[^a-zA-Z0-9]', ' ', text)


        def remove_links(self, text):
            """Remove URLs from text"""
            if not text:
                return text

            # Common URL pattern
            url_pattern = r'http[s]?://(?:[a-zA-Z]|[0-9]|[$-_@.&+]|[!*\(\),]|(?:%[0-9a-fA-F][0-9a-fA-F]))+'

            # Remove URLs
            text = re.sub(url_pattern, '', text)

            # Also try to remove other possible link formats
            text = re.sub(r'www\.[^\s]+', '', text)

            return text.strip()

        def convert_date_fields(self, transformed):
            """Convert date strings to datetime objects"""
            date_fields = ['date_published', 'date_expiration']

            for field in date_fields:
                if field in transformed and transformed[field]:
                    try:
                        # Try to parse the date string
                        parsed_date = parser.parse(transformed[field])
                        transformed[field] = parsed_date.isoformat()
                    except (ValueError, TypeError):
                        # If parsing fails, set as not supported
                        transformed[field] = "not supported"


        def extract_domain(self, url):
            """Extract domain name from URL"""
            try:
                parsed_url = urlparse(url)
                # Get domain with subdomain (e.g., trabajito.com.bo)
                domain = parsed_url.netloc
                return domain
            except:
                return None


        def get_job_status(self, expiration_date):
            """Determine if job is active or expired based on expiration date"""
            try:
                # Parse the expiration date
                exp_date = parser.parse(expiration_date)

                # Compare with current date
                current_date = datetime.now()

                # Return status
                if exp_date > current_date:
                    return "Active"
                else:
                    return "Expired"
            except:
                # If date parsing fails, default to "Unknown"
                return "Unknown"


