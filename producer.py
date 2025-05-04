import requests
import json
import time
import logging
from os import getenv
from datetime import date, timedelta # <-- Thêm import này
from kafka import KafkaProducer
from kafka.errors import KafkaError, NoBrokersAvailable

# ---- Cấu hình Logging ----
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# ---- Cấu hình API và Kafka ----
API_ENDPOINT = "https://api.twitterapi.io/twitter/tweet/advanced_search"
KAFKA_BOOTSTRAP_SERVERS = 'localhost:9092'
PAGE_DELAY_SECONDS = 1
INTER_QUERY_DELAY_SECONDS = 30
TARGET_TWEET_COUNT_PER_TOPIC = 800

# ---- Tự động tạo SEARCH_CONFIGS ----
KEYWORDS_TOPICS = [ # Danh sách các cặp keyword - topic (theo thứ tự)
    ('gpt', 'gpt-2'),
    ('copilot', 'copilot-2'),
    ('gemini', 'gemini-2')
]
# Chat GPT 4o (13/4/2024)
# START_DATE_STR = '2024-05-06'
# END_DATE_STR = '2024-05-20'
# Gemini Advanced 2.5 (25/03/2025)
START_DATE_STR = '2025-03-18'
END_DATE_STR = '2025-04-01'
LANG = 'en'

SEARCH_CONFIGS = [] # Khởi tạo danh sách rỗng

try:
    start_date = date.fromisoformat(START_DATE_STR)
    end_date = date.fromisoformat(END_DATE_STR)
    one_day = timedelta(days=1)

    current_date = start_date
    while current_date <= end_date:
        next_date = current_date + one_day
        since_str = current_date.strftime('%Y-%m-%d')
        until_str = next_date.strftime('%Y-%m-%d')

        for keyword, topic in KEYWORDS_TOPICS:
            config = {
                'keyword': keyword,
                'topic': topic,
                'query_params': {
                    'lang': LANG,
                    'since': since_str,
                    'until': until_str
                }
            }
            SEARCH_CONFIGS.append(config)

        current_date += one_day # Chuyển sang ngày tiếp theo

    logging.info(f"Đã tự động tạo {len(SEARCH_CONFIGS)} cấu hình tìm kiếm từ {START_DATE_STR} đến {END_DATE_STR}.")
    # Bạn có thể bỏ comment dòng dưới để xem vài config đầu tiên
    # if SEARCH_CONFIGS: logging.debug(f"Ví dụ cấu hình: {SEARCH_CONFIGS[:3]}")

except ValueError:
    logging.error(f"Lỗi định dạng ngày không hợp lệ: '{START_DATE_STR}' hoặc '{END_DATE_STR}'. Vui lòng dùng định dạng YYYY-MM-DD.")
    exit(1)
except Exception as e:
    logging.error(f"Lỗi không xác định khi tạo SEARCH_CONFIGS: {e}")
    exit(1)

# ---- Tải API Key từ .env hoặc biến môi trường ----
# ... (Phần còn lại của code giữ nguyên) ...
try:
    # Sử dụng os.environ.get để tránh lỗi nếu biến không tồn tại
    API_KEY = getenv("TWITTERAPI_IO_KEY")
    if not API_KEY:
        raise ValueError("Biến môi trường TWITTERAPI_IO_KEY không được đặt hoặc trống.")
except ValueError as e:
    logging.error(f"Lỗi: {e}")
    exit(1)
except Exception as e:
    logging.error(f"Lỗi không xác định khi đọc API Key: {e}")
    exit(1)

# ---- Khởi tạo Kafka Producer ----
# ... (Phần còn lại của code giữ nguyên) ...
kafka_producer = None # Khởi tạo là None
try:
    kafka_producer = KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode('utf-8'),
        # Thêm timeout để kiểm tra kết nối nhanh hơn
        request_timeout_ms=10000 # 10 giây
    )
    # Thử gửi một bản ghi metadata để kiểm tra kết nối sớm
    kafka_producer.partitions_for('dummy_topic_check') # Sẽ báo lỗi nếu không kết nối được
    logging.info(f"Đã kết nối tới Kafka broker tại {KAFKA_BOOTSTRAP_SERVERS}")
except NoBrokersAvailable:
     logging.error(f"Lỗi: Không thể kết nối tới Kafka broker tại {KAFKA_BOOTSTRAP_SERVERS}. Vui lòng kiểm tra Kafka đang chạy và địa chỉ đúng.")
     exit(1)
except Exception as e:
    logging.error(f"Lỗi khi khởi tạo Kafka Producer hoặc kiểm tra kết nối: {e}")
    exit(1)


# ---- Hàm kết nối API ----
# ... (Giữ nguyên) ...
def make_api_request(params):
    """Gửi yêu cầu đến twitterapi.io và trả về JSON"""
    headers = {'X-API-Key': API_KEY}
    logging.debug(f"Đang gửi yêu cầu tới {API_ENDPOINT} với params: {params}")
    try:
        response = requests.get(API_ENDPOINT, headers=headers, params=params, timeout=60) # Timeout 60 giây
        logging.debug(f"Trạng thái phản hồi: {response.status_code}")
        response.raise_for_status()  # Ném lỗi nếu status code là lỗi (>=400)
        return response.json()
    except requests.exceptions.Timeout:
        logging.error("Lỗi: Yêu cầu API hết thời gian chờ (Timeout).")
        return None # Trả về None để xử lý bên ngoài
    except requests.exceptions.HTTPError as http_err:
        logging.error(f"Lỗi HTTP từ API: {http_err.response.status_code} - {http_err}")
        try:
            logging.error(f"Chi tiết lỗi từ server: {http_err.response.text}")
        except:
            pass
        return None # Trả về None
    except requests.exceptions.RequestException as e:
        logging.error(f"Lỗi kết nối hoặc yêu cầu API khác: {e}")
        return None # Trả về None
    except json.JSONDecodeError:
        logging.error("Lỗi: Không thể phân tích phản hồi JSON từ API.")
        logging.error(f"Nội dung phản hồi nhận được (nếu có): {response.text if 'response' in locals() else 'Không có'}")
        return None # Trả về None

# ---- Hàm gửi dữ liệu vào Kafka ----
# ... (Giữ nguyên) ...
def send_to_kafka(producer, topic, tweets_list):
    """Gửi danh sách tweets vào Kafka topic"""
    if not producer or not tweets_list:
        return 0

    send_count = 0
    for tweet in tweets_list:
        try:
            # Gửi toàn bộ đối tượng tweet vào Kafka
            future = producer.send(topic, value=tweet)
            # Optional: Add callbacks for success/error handling per message
            # future.add_callback(on_send_success, tweet_id=tweet.get('id'))
            # future.add_errback(on_send_error, tweet_id=tweet.get('id'))
            logging.debug(f"Đã đưa tweet ID {tweet.get('id', 'N/A')} vào buffer gửi đến topic '{topic}'")
            send_count += 1
        except KafkaError as ke:
            logging.error(f"Lỗi Kafka khi gửi tweet ID {tweet.get('id', 'N/A')} vào topic '{topic}': {ke}")
        except Exception as e:
            logging.error(f"Lỗi không xác định khi gửi tweet ID {tweet.get('id', 'N/A')} vào topic '{topic}': {e}")

    if send_count > 0:
         logging.info(f"Đã đưa {send_count} tweets vào buffer gửi đến topic '{topic}'")
         # Quan trọng: Flush để đảm bảo message được gửi đi (hoặc gọi định kỳ/cuối cùng)
         # producer.flush() # Cân nhắc flush ở đây hoặc cuối vòng lặp lớn/cuối script
    return send_count


# ---- Hàm xử lý lấy dữ liệu cho một cấu hình cụ thể ----
# ... (Giữ nguyên) ...
def process_search_config(config, producer, target_count):
    """Lấy dữ liệu từ API và gửi vào Kafka cho một cấu hình search"""
    keyword = config['keyword']
    topic_name = config['topic']
    # Xây dựng query string hoàn chỉnh
    query_parts = [f'"{keyword}"'] # Đảm bảo keyword trong ngoặc kép
    query_date_info = "" # Thêm thông tin ngày vào log
    if 'query_params' in config:
        for key, value in config['query_params'].items():
            if key == 'lang':
                 query_parts.append(f"lang:{value}")
            elif key == 'since':
                query_parts.append(f"since:{value}")
                query_date_info = f" (Ngày: {value})" # Lấy ngày since để log
            elif key == 'until':
                query_parts.append(f"until:{value}")

    query_string = " ".join(query_parts)

    # Cập nhật log để bao gồm thông tin ngày
    logging.info(f"\n{'='*15} Bắt đầu xử lý cho Keyword: '{keyword}'{query_date_info}, Topic: '{topic_name}' {'='*15}")
    logging.info(f"Query: {query_string}")
    logging.info(f"Mục tiêu: Tối thiểu {target_count} tweets.")
    logging.info(f"Gửi dữ liệu vào Kafka topic: {topic_name}")
    logging.info("-" * 50)

    cursor = None
    collected_tweet_count = 0
    page_number = 0
    consecutive_api_errors = 0
    max_consecutive_api_errors = 3 # Dừng nếu lỗi API liên tiếp

    # Tham số truy vấn cơ bản
    base_params = {
        'query': query_string,
        'queryType': 'Top' # Hoặc 'Live' nếu bạn muốn tweet mới nhất
    }

    while collected_tweet_count < target_count:
        page_number += 1
        # Cập nhật log trang để bao gồm thông tin ngày
        logging.info(f"\n--- Chuẩn bị lấy trang {page_number} cho '{keyword}'{query_date_info} ---")

        current_params = base_params.copy()
        if cursor:
            logging.info(f"Sử dụng cursor từ trang trước: {cursor[:10]}...") # Log một phần cursor
            current_params['cursor'] = cursor
        else:
            logging.info("Yêu cầu trang đầu tiên.")

        try:
            json_response = make_api_request(current_params)

            if json_response is None: # Xử lý lỗi từ make_api_request
                logging.warning(f"Không nhận được phản hồi hợp lệ từ API cho '{keyword}'{query_date_info}.")
                consecutive_api_errors += 1
                if consecutive_api_errors >= max_consecutive_api_errors:
                    logging.error(f"Đã gặp lỗi API {max_consecutive_api_errors} lần liên tiếp. Dừng xử lý cho '{keyword}'{query_date_info}.")
                    break # Thoát vòng lặp while cho config này
                # Chờ một chút trước khi thử lại
                logging.info(f"Chờ {PAGE_DELAY_SECONDS * 2} giây trước khi thử lại API...")
                time.sleep(PAGE_DELAY_SECONDS * 2)
                continue # Bỏ qua lần lặp này và thử lại

            # Reset bộ đếm lỗi nếu thành công
            consecutive_api_errors = 0

            tweets_on_page = json_response.get('tweets', [])
            has_next = json_response.get('has_next_page', False)
            next_cursor = json_response.get('next_cursor')

            result_count_on_page = len(tweets_on_page)

            if result_count_on_page > 0:
                logging.info(f"API trả về {result_count_on_page} tweets trên trang này cho '{keyword}'{query_date_info}.")

                sent_count = send_to_kafka(producer, topic_name, tweets_on_page)
                collected_tweet_count += sent_count
                logging.info(f"Tổng số tweets đã đưa vào buffer cho '{topic_name}': {collected_tweet_count}") # Giữ nguyên log này, không cần thêm ngày vì topic là chung

                cursor = next_cursor

                if collected_tweet_count >= target_count:
                    logging.info(f"\n*** Đã đạt hoặc vượt mục tiêu {target_count} tweets cho '{keyword}'{query_date_info}. Dừng lại. ***")
                    break
                if not has_next or not cursor:
                    logging.info(f"\n*** API báo không còn trang tiếp theo hoặc không có cursor cho '{keyword}'{query_date_info}. Dừng lại. ***")
                    break

                logging.info(f"\n--- Đợi {PAGE_DELAY_SECONDS} giây trước khi lấy trang tiếp theo cho '{keyword}'{query_date_info}... ---")
                time.sleep(PAGE_DELAY_SECONDS)

            else:
                logging.warning(f"\n*** Không tìm thấy kết quả nào trên trang này từ API cho '{keyword}'{query_date_info}. ***")
                if 'cursor' not in current_params:
                     logging.warning(f"*** Không có kết quả nào cho truy vấn ban đầu '{keyword}'{query_date_info}. ***")
                else:
                     logging.warning(f"*** Có thể đã hết kết quả cho '{keyword}'{query_date_info} từ trang trước đó. ***")
                break # Dừng nếu không có kết quả

        except Exception as e:
            logging.error(f"\n--- !!! Đã xảy ra lỗi không mong muốn trong vòng lặp xử lý trang cho '{keyword}'{query_date_info} !!! ---")
            logging.error(f"Lỗi: {e}")
            logging.warning(f"--- Dừng xử lý cho '{keyword}'{query_date_info}. ---")
            # import traceback
            # logging.error(traceback.format_exc())
            break # Thoát vòng lặp while cho config này

    # Cập nhật log hoàn tất để bao gồm thông tin ngày
    logging.info(f"{'='*15} Hoàn tất xử lý cho Keyword: '{keyword}'{query_date_info}. Tổng cộng đã đưa vào buffer: {collected_tweet_count} tweets. {'='*15}\n")
    return collected_tweet_count


# ---- Hàm chính ----
# ... (Giữ nguyên) ...
def main():
    total_tweets_processed_all_configs = 0

    # Sử dụng try...finally để đảm bảo producer được đóng ngay cả khi có lỗi
    global kafka_producer # Sử dụng producer đã khởi tạo ở global scope
    try:
        # Lấy keyword từ config để hiển thị trong log flush và delay
        current_keyword = ""
        current_date_str = "" # Lấy ngày từ config
        for i, config in enumerate(SEARCH_CONFIGS):
            # Cập nhật keyword và ngày hiện tại từ config
            current_keyword = config.get('keyword', 'N/A')
            if 'query_params' in config and 'since' in config['query_params']:
                 current_date_str = f" (Ngày: {config['query_params']['since']})"
            else:
                 current_date_str = ""

            count_for_config = process_search_config(config, kafka_producer, TARGET_TWEET_COUNT_PER_TOPIC)
            total_tweets_processed_all_configs += count_for_config

            # Flush sau mỗi config để đảm bảo dữ liệu được gửi đi
            if count_for_config > 0:
                 # Cập nhật log flush để rõ ràng hơn
                 logging.info(f"Flushing Kafka producer sau khi xử lý '{current_keyword}'{current_date_str}...")
                 kafka_producer.flush(timeout=30) # Chờ tối đa 30s để flush
                 logging.info("Flush hoàn tất.")

            # Thêm độ trễ giữa các lần xử lý query khác nhau
            # Sử dụng i để kiểm tra nếu không phải config cuối cùng
            if i < len(SEARCH_CONFIGS) - 1:
                logging.info(f"\n--- Đợi {INTER_QUERY_DELAY_SECONDS} giây trước khi xử lý query tiếp theo... ---\n")
                time.sleep(INTER_QUERY_DELAY_SECONDS)

    except Exception as e:
        logging.error(f"Lỗi nghiêm trọng trong hàm main: {e}")
        # import traceback
        # logging.error(traceback.format_exc())

    finally:
        # Đảm bảo gửi hết các message còn lại trong buffer và đóng kết nối
        if kafka_producer:
            logging.info("Bắt đầu quá trình đóng Kafka producer...")
            try:
                logging.info("Flushing lần cuối cùng...")
                kafka_producer.flush(timeout=60) # Tăng timeout cho lần flush cuối
                logging.info("Flush cuối cùng hoàn tất.")
            except Exception as flush_err:
                logging.error(f"Lỗi trong quá trình flush cuối cùng: {flush_err}")
            finally:
                logging.info("Closing Kafka producer...")
                kafka_producer.close(timeout=30) # Chờ đóng tối đa 30s
                logging.info("Kafka producer đã đóng.")

    logging.info("-" * 60)
    logging.info(f"Hoàn tất toàn bộ quá trình! Đã xử lý và đưa vào buffer tổng cộng {total_tweets_processed_all_configs} tweets qua các cấu hình.")
    logging.info("-" * 60)


if __name__ == "__main__":
    main()