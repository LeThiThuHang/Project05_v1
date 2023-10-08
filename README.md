# Project05_v1
 Yêu cầu: Đồng bộ toàn bộ dữ liệu sản phẩm Tiki (Project 04) lên Data Warehouse BigQuery.

Mô tả cụ thể:

- Tạo Compute Engine và cài đặt MongoDB, sau đó restore toàn bộ dữ liệu Tiki từ MongoDB local lên MongoDB trên VM
- Tạo backup dữ liệu bằng việc đồng bộ toàn bộ sản phẩm đang lưu trữ ở MongoDB vào Google Cloud Storage.
- Thiết kế Data Warehouse - BigQuery để load toàn bộ dữ liệu của Tiki
- Tạo một table data mart về seller và các sản phẩm mà seller đang bán để cho phía DA sử dụng
- Kết nối với Data Studio. Tạo dashboard cơ bản thể hiện:
    - Số lượng sản phẩm đã bán của các danh mục lớn
    - Các nhãn hàng xuất xứ từ Trung Quốc phân bố ở các danh mục lớn ra sao
    - Mối tương quan giữa rating sản phẩm với giá sản phẩm.
    - Top 10 seller nhiều sản phẩm nhất trên Tiki, số lượng là bao nhiêu
- Tự động hoá toàn bộ các yêu cầu trên với Crontab và Cloud Function. Cụ thể: Đặt lịch lúc 20h để dữ liệu đẩy từ MongoDB lên GCS. Khi GCS có dữ liệu mới, tự động trigger Cloud Function load data từ GCS vào BigQuery
- Propose cho leader các chiến lược về tối ưu chi phí các components đang sử dụng sao cho hiệu quả
