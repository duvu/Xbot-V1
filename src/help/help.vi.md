# Hướng dẫn sử dụng QMVBot

## Giới thiệu
QMVbot là một ứng dụng của nhóm QMV được phát triển nhằm mục đích hỗ trợ các nhà đầu tư tiếp cận tốt nhất với các thông tin tài chính và tự xâ dựng phương pháp đầu tư tối ưu cho riêng mình.

## Các lệnh của QMVbot
1. Định giá

Bot hỗ trợ định giá các mã cổ phiếu bằng phương pháp so sánh.

Cú pháp

```?dinhgia -t"mã cần định giá"+"mã so sánh 1"+"mã so sánh 2"```

Ví dụ:

```?dinhgia -tmbb vpb tcb```


2. Xác định xu hướng trend

Bot tìm kiếm các trao đổi trên các diễn đàn phổ biến

Cú pháp

```?trending "trong vòng mấy giờ" "số mã cần xem"```

Ví dụ:

```?trending 8 5``` : xem xu hướng đc đề cập nhiều trong 8h của 5 mã

```?trending QTP```: xem bao nhiều lần QTP đc đề cập trong 24h qua

```?trending``` : bot sẽ mặc định tra 24h qua 10 mã đc đề cập nhiều nhất


3. Tối ưu hóa danh mục: 

Xây dựng danh mục tối ưu trong phạm vi đầu tư trên 120 ngày.

Cú pháp:
``` ?mpt mã_1 mã_2 mã_3 ...```

Ví dụ:

```?mpt vpb mbb tcb ```: hệ thống giúp phân bổ bao nhiêu % cho từng mã, tối ưu danh mục tốt hơn


4. Thông tin về cổ phiếu cụ thế

Cú pháp

```?info +mã_cổ_phiếu```

Ví dụ

```?info HPG```