import asyncio
import datetime
import io
import json
import logging
import os
import platform
from urllib.parse import urlparse, urlencode
from typing import Optional

from aiohttp import ClientSession
from aiolimiter import AsyncLimiter
from tqdm.auto import tqdm
from tqdm_logger import TqdmToLogger

from utils import s3_key_exists, get_s3_client, write_to_s3, download

# setup logging here
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - [%(levelname)s] - %(message)s",
    handlers=[logging.FileHandler("subs_logs.log"), logging.StreamHandler()],
)

tqdm_logger = TqdmToLogger(logger=logging, level=logging.INFO)

# setup async windows settings here settings here
system = platform.system().lower()
if system == "windows":
    asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

# set up limiter
limiter = AsyncLimiter(max_rate=1, time_period=2)


# For all submissions
def get_submission_search_urls(sdate: str, edate: str) -> list:
    base_url = "https://api.pushshift.io/reddit/search/submission/?"
    # submission_status_df = gather_wsb.get_submission_status_mat_view(local=True)

    sdate, edate = datetime.datetime.strptime(sdate, "%Y-%m-%d"), datetime.datetime.strptime(edate, "%Y-%m-%d")
    date_range = [edate - datetime.timedelta(days=x) for x in range((edate - sdate).days)]

    # For missing_start_end_dates
    missing_dates = list(set(date_range))
    missing_dates_pairs = [
        (
            str(int((x - datetime.timedelta(days=1)).timestamp())),
            str(int((x + datetime.timedelta(days=1)).timestamp())),
        )
        for x in missing_dates
    ]

    # Make all params
    all_urls = []
    for start, end in missing_dates_pairs:
        parsed = urlparse(base_url)
        params = {
            "subreddit": "wallstreetbets",
            "after": start,
            "before": end,
            "sort_type": "num_comments",
            "sort": "desc",
            "limit": "1000",
        }
        params = urlencode(params)
        parsed = parsed._replace(query=params)
        all_urls.append(parsed.geturl())

    return all_urls


async def extract_submissions(
        sdate: str, edate: str, urls=None
) -> [list, list]:
    if urls is None:
        urls = get_submission_search_urls(sdate=sdate, edate=edate)

    cols = ["created_utc", "id", "author", "url", "title", "selftext", "stickied"]
    not_errors, errors = [], []
    async with ClientSession() as session:
        tasks = [asyncio.create_task(download(url, session)) for url in urls]

        for future in tqdm(asyncio.as_completed(tasks), total=len(urls), file=tqdm_logger, desc="Downloading all submissions..."):
            res = await future

            if "error_url" in res.keys():
                errors.append(res["error_url"])

            elif "data" in res.keys():
                res = res["data"]

                for result in res:
                    r = {
                        col: (result[col] if col in result.keys() else None)
                        for col in cols
                    }
                    not_errors.append(r)
    return not_errors, errors


########################################################################################################################
def download_submission_comments_status_df():
    conn_string = f"postgresql://postgres:rogerthat@127.0.0.1:5432/TimeScaleDB"
    engine = sqlalchemy.create_engine(conn_string).execution_options(autocommit=True)
    df = pd.read_sql_table(
        table_name="submission_comments_status",
        con=engine,
    )
    df = df.set_index("date")
    df.loc[:, "comments_len"] = df["all_comments_found"].apply(
        lambda x: len(np.unique(x))
    )
    df.to_csv("submission_comments_status.csv.gzip", compression="gzip")


def get_comments_ids_search_urls(sdate: str, edate: str) -> list:
    submission_comments_status_df = pd.read_csv("submission_comments_status.csv.gzip", index_col=0, compression="gzip")

    submission_ids = submission_comments_status_df.loc[
                     sdate:edate, "submission_id"
                     ].values.tolist()

    search_comments_base_url = "https://api.pushshift.io/reddit/submission/comment_ids"
    all_urls = []

    for submission_id in submission_ids:
        all_urls.append(f"{search_comments_base_url}/{submission_id}")

    return all_urls


async def make_urls_using_sub_id_for_comments(
        sdate: str, edate: str, comments_ids_urls: list = None
) -> Optional[list]:
    search_comments_base_url = "https://api.pushshift.io/reddit/comment/search"

    if comments_ids_urls is None:
        comments_ids_urls = get_comments_ids_search_urls(
            sdate=sdate, edate=edate
        )
    else:
        if uri_validate(comments_ids_urls[0]):
            pass
        else:
            return None

    all_urls = []
    tasks = [
        asyncio.create_task(download_modified(url=url, return_sub_id_from_url=True))
        for url in comments_ids_urls
    ]

    for future in tqdm(
            asyncio.as_completed(tasks),
            total=len(comments_ids_urls),
            file=tqdm_logger,
            desc="Searching for comments ids within submissions",
    ):
        results = await future

        if results and ("data" in results.keys()):
            sub_id = results["submission_id"]
            results = results["data"]

            for id_batch in batch(results, n=400):
                ids = ",".join(id_batch)
                all_urls.append({sub_id: f"{search_comments_base_url}?ids={ids}"})

    return all_urls


async def extract_comments(urls: list) -> list:
    cols = [
        "created_utc",
        "retrieved_on",
        "id",
        "parent_id",
        "link_id",
        "author",
        "submission_id",
        "body",
        "subreddit",
    ]

    tasks = [
        asyncio.create_task(download_modified(url=url, return_sub_id_from_dict=True))
        for url in urls
    ]

    all_results = []
    for future in tqdm(
            asyncio.as_completed(tasks),
            total=len(urls),
            file=tqdm_logger,
            desc="Downloading comments in batches...",
    ):
        results = await future

        if results:
            sub_id = results["submission_id"]
            results = results["data"]

            for result in results:
                r = {
                    col: (result[col] if col in result.keys() else None) for col in cols
                }
                r["submission_id"] = sub_id
                all_results.append(r)

    return all_results


if __name__ == "__main__":

    logging.info("Fetching s3 client...")
    s3_client = get_s3_client(logger=logging)
    logging.info("Fetched!...\n")

    # get start and end date from sys env variables
    logging.info("Fetch start/end dates and program types...")
    start_date, end_date, exec_type = os.getenv("SDATE"), os.getenv("EDATE"), os.getenv("EXEC_TYPE")
    logging.info("Fetched!...\n")

    if not (start_date and end_date):
        logging.error("ENV vars SDate and EDate not set.")
        raise (EnvironmentError, "ENV vars SDate and EDate not set.")

    if exec_type == "UPDATE_SUB":
        logging.info("Downloading local submission_comments_status and writing to file...")
        download_submission_comments_status_df()
        logging.info("Written!...\n")

    if exec_type == "COMMENTS":
        logging.info("Making urls for fetching comments, using submission ids...")
        comments_urls_with_sub_ids = asyncio.run(
            make_urls_using_sub_id_for_comments(start_date=sdate, end_date=edate)
        )
        logging.info("Fetched!...\n")

        logging.info("Extracting comments...")
        comments_list = asyncio.run(
            extract_comments(urls=comments_urls_with_sub_ids)
        )
        logging.info("Fetched!...\n")

        upload_key = f"comments_list_from_{sdate}_{edate}.json.gzip"
        logging.info(f"Uploading to S3://polygonio-dumps/{upload_key}")
        upload_json_gz_to_s3(key=upload_key, obj=comments_list)
        logging.info("Uploaded!\n")

    if exec_type == "SUB":
        logging.info("Extract submissions...")
        all_results, error_urls = asyncio.run(
            extract_submissions(sdate=start_date, edate=end_date)
        )
        logging.info("Extracted!\n")

        # check if a file like "error urls" exists in s3, else just look for start/end date env variables
        logging.info("Checking if error urls for the same dates exist...")
        submission_errors_urls_keys = f"submission_error_urls_{start_date}_{end_date}.json"
        if s3_key_exists(
                client=s3_client,
                bucket="polygonio-dumps",
                key=submission_errors_urls_keys,
        ):
            logging.info(f"{submission_errors_urls_keys} exists and downloading!")
            jsonBuffer = io.StringIO()
            s3_client.download_file(
                "polygonio-dumps",
                submission_errors_urls_keys,
                jsonBuffer,
            )
            submission_error_urls = json.load(jsonBuffer)["all_submission_errors"]

            logging.info("Extract submissions for those error urls...")
            results_1, error_urls_1 = asyncio.run(extract_submissions(urls=submission_error_urls))
            logging.info("Extracted error urls!\n")

            all_results += results_1
            error_urls += error_urls_1

        # write both error urls and all_results to s3
        submission_results_key = f"submission_results_{start_date}_{end_date}.json"
        logging.info(f"Uploading all results to s3://polygonio-dumps/{submission_results_key}...")
        write_to_s3(key=submission_results_key, results=all_results)
        logging.info("Uploaded!\n")

        logging.info(f"Uploading all results to s3://polygonio-dumps/{submission_errors_urls_keys}")
        write_to_s3(key=submission_errors_urls_keys, errors=error_urls)
        logging.info("Uploaded!\n")
