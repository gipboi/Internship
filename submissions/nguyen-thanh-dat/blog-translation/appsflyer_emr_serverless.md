# Di chuyển dữ liệu quy mô petabyte trở nên đơn giản: Hành trình thực tiễn của AppsFlyer với Amazon EMR Serverless


> **📖 Bài viết gốc**: https://aws.amazon.com/blogs/big-data/petabyte-scale-data-migration-made-simple-appsflyers-best-practice-journey-with-amazon-emr-serverless/  
> **👤 Tác giả**: Roy Ninio, Avichay Marciano, Eitav Arditti và Yonatan Dolan  
> **📅 Ngày xuất bản**: 12/05/2025  
> **🌐 Nguồn**: AWS Big Data Blog  
> **👨‍💻 Người dịch**: Nguyễn Thành Đạt – FCJ Trainee  
> **📅 Ngày dịch**: 03/07/2025  
> **⏱️ Thời gian đọc**: 38 phút đọc


---


## 📚 Mục lục

- [1. Giới thiệu và bối cảnh](#1-giới-thiệu-và-bối-cảnh)
- [2. Lý do chọn EMR Serverless](#2-lý-do-chọn-emr-serverless)
- [3. Kiến trúc hệ thống cũ và mới](#3-kiến-trúc-hệ-thống-cũ-và-mới)
- [4. Chiến lược chuyển đổi](#4-chiến-lược-chuyển-đổi)
- [5. CI/CD cho Spark job](#5-cicd-cho-spark-job)
- [6. Operator Airflow tùy chỉnh](#6-operator-airflow-tùy-chỉnh)
- [7. Quản lý phân quyền đa tài khoản](#7-quản-lý-phân-quyền-đa-tài-khoản)
- [8. Tích hợp Lineage với Spline](#8-tích-hợp-lineage-với-spline)
- [9. Giám sát và observability](#9-giám-sát-và-observability)
- [10. Tổng kết](#10-tổng-kết)
- [📖 Tài liệu tham khảo](#📖-tài-liệu-tham-khảo)


---

## 1. Giới thiệu và bối cảnh
Trên toàn thế giới, các tổ chức đang không ngừng nỗ lực khai thác sức mạnh của dữ liệu để đưa ra các quyết định thông minh và chính xác hơn bằng cách tích hợp dữ liệu vào trọng tâm hoạt động của mình. Khi tận dụng tốt các thông tin dựa trên dữ liệu, doanh nghiệp có thể phản ứng nhanh hơn trước những biến động, thúc đẩy đổi mới và nâng cao trải nghiệm cho khách hàng.

Thực tế cho thấy, dữ liệu đã thay đổi cách các tổ chức đưa ra quyết định. Tuy nhiên, việc quản lý cơ sở hạ tầng phục vụ xử lý dữ liệu lại luôn là một thử thách lớn, đòi hỏi kỹ năng chuyên môn cao và đội ngũ kỹ thuật riêng biệt. Những khó khăn trong việc thiết lập, mở rộng và duy trì các hệ thống dữ liệu quy mô lớn thường làm giảm tính linh hoạt, cản trở tốc độ đổi mới và tiêu tốn nguồn lực đáng kể.



### AppsFlyer là ai?

AppsFlyer là công ty hàng đầu về phân tích và đo lường hiệu quả tiếp thị, giúp các doanh nghiệp đánh giá và tối ưu các chiến dịch marketing trên thiết bị di động, web và các thiết bị kết nối. Với triết lý phát triển đặt quyền riêng tư người dùng lên hàng đầu, AppsFlyer trao cho doanh nghiệp quyền chủ động ra quyết định dựa trên dữ liệu mà vẫn đảm bảo tuân thủ các quy định về quyền riêng tư.

AppsFlyer cung cấp các công cụ theo dõi hành vi người dùng – từ giai đoạn thu hút đến duy trì và tương tác – từ đó mang lại các thông tin thiết thực giúp tăng lợi tức đầu tư (ROI) và tối ưu hoá chiến lược marketing.



### Trong bài viết này
Chúng tôi sẽ chia sẻ cách AppsFlyer đã di chuyển toàn bộ hạ tầng dữ liệu khổng lồ của mình – từ các cụm Hadoop tự vận hành sang Amazon EMR Serverless. Bài viết này trình bày các thực tiễn tốt nhất mà họ áp dụng, những thách thức trong quá trình chuyển đổi và những bài học quan trọng có thể giúp các tổ chức khác thực hiện thành công các dự án tương tự.

## 2. Lý do chọn EMR Serverless

### Tại sao AppsFlyer lại chọn hướng tiếp cận “serverless” cho dữ liệu lớn?

AppsFlyer hiện đang vận hành một trong những hệ thống xử lý dữ liệu có quy mô lớn nhất trong ngành, xử lý khoảng 100 petabyte dữ liệu mỗi ngày, tiếp nhận hàng triệu sự kiện mỗi giây và chạy hàng nghìn tác vụ trên gần 100 cụm Hadoop tự vận hành.

Kiến trúc hệ thống của họ tích hợp nhiều công nghệ mã nguồn mở trong lĩnh vực kỹ thuật dữ liệu như Apache Spark, Apache Kafka, Apache Iceberg và Apache Airflow. Dù đã hoạt động ổn định trong nhiều năm, hệ thống này ngày càng trở nên khó mở rộng linh hoạt theo nhu cầu, đồng thời phát sinh nhiều chi phí và công sức để duy trì. Điều này đã thôi thúc AppsFlyer suy nghĩ lại toàn bộ chiến lược xử lý dữ liệu lớn.

Amazon EMR Serverless là giải pháp hiện đại, có khả năng mở rộng linh hoạt, loại bỏ hoàn toàn nhu cầu quản lý cụm thủ công, đồng thời tự động điều chỉnh tài nguyên theo đúng nhu cầu xử lý thực tế. Nhờ vậy, khả năng mở rộng hoặc thu hẹp tài nguyên diễn ra chỉ trong vài giây, hạn chế tối đa thời gian chờ và tránh được các sự cố như mất phiên làm việc do sử dụng Spot instance.

Việc chuyển sang EMR Serverless đã giúp đội ngũ kỹ thuật của AppsFlyer giảm gánh nặng hạ tầng để tập trung nhiều hơn vào đổi mới, tăng độ ổn định hệ thống, đồng thời đảm bảo kiến trúc có thể mở rộng trong tương lai. Đặc biệt, họ chỉ phải trả phí cho CPU và bộ nhớ thực sự sử dụng trong quá trình thực thi, từ đó tối ưu hóa chi phí, loại bỏ sự lãng phí do tài nguyên rỗi.


## 3. Kiến trúc hệ thống cũ và mới
### Tổng quan giải pháp

Trước đây, AppsFlyer vận hành các cụm Hadoop tự quản lý trên Amazon EC2 để xử lý toàn bộ quy trình dữ liệu phức tạp của mình. Dù hệ thống này đáp ứng được yêu cầu vận hành, nhưng lại tiêu tốn nhiều công sức để duy trì, mở rộng và tối ưu.

Mỗi ngày, AppsFlyer điều phối hơn 100.000 luồng công việc với Airflow, bao gồm cả pipeline streaming và batch.

Đối với streaming, họ sử dụng Spark Streaming để lấy dữ liệu thời gian thực từ Kafka, ghi dữ liệu thô vào Amazon S3, đồng thời đưa lên BigQuery và Google Cloud Storage để xây dựng tầng dữ liệu logic.


Đối với batch, dữ liệu thô được xử lý, chuyển thành dữ liệu có cấu trúc phục vụ cho các nhóm nội bộ, dashboard và quy trình phân tích. Một phần kết quả cũng được đẩy ra các nguồn dữ liệu bên ngoài để cung cấp insight cho khách hàng.


Đối với truy vấn thời gian thực, họ sử dụng ClickHouse và Druid để hiển thị dashboard. Các bảng Iceberg được tạo từ dữ liệu Delta Lake và truy vấn qua Amazon Athena.


Khi chuyển sang EMR Serverless, AppsFlyer loại bỏ hoàn toàn cụm Hadoop tự quản, cải thiện đáng kể khả năng mở rộng, tiết kiệm chi phí và đơn giản hóa vận hành.

Các pipeline Spark – cả streaming lẫn batch – đều được di chuyển sang EMR Serverless để tận dụng tính co giãn động của nền tảng này, giúp xử lý hiệu quả hơn.

Việc chuyển đổi này đã giảm đáng kể chi phí vận hành, giải phóng đội ngũ kỹ sư khỏi việc quản lý cụm thủ công để tập trung vào xử lý dữ liệu.



![alt text](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/High-level-diagram.png)
## 4. Chiến lược chuyển đổi
### Những thử thách và bài học rút ra

Việc chuyển đổi toàn bộ tổ chức lớn như AppsFlyer từ Hadoop sang EMR Serverless là một thử thách thực sự, đặc biệt vì nhiều nhóm R&D chưa có kinh nghiệm vận hành hạ tầng.

Để đảm bảo chuyển đổi thành công, nhóm Hạ tầng Dữ liệu (DataInfra) đã triển khai một chiến lược chuyển đổi toàn diện, giúp các nhóm R&D tự tin và chủ động di chuyển pipeline của mình.

#### Chuẩn bị tập trung từ DataInfra

- **Phân công rõ ràng**: Nhóm DataInfra đảm nhận toàn bộ trách nhiệm về quá trình di chuyển, từ lập kế hoạch, hướng dẫn cho đến hỗ trợ các nhóm.


- **Tài liệu chi tiết**: Họ xây dựng hướng dẫn từng bước rõ ràng, đơn giản hóa quá trình di chuyển từ Hadoop để các nhóm dễ dàng tiếp cận dù chưa có nền tảng kỹ thuật hạ tầng.


#### Thiết lập mạng lưới hỗ trợ nội bộ

- **Cộng đồng dữ liệu**: Là nơi các nhóm đặt câu hỏi và chia sẻ kinh nghiệm. Nhóm DataInfra chủ động dẫn dắt cộng đồng này.


- **Kênh Slack hỗ trợ riêng**: Hỗ trợ thời gian thực, giải đáp thắc mắc nhanh chóng để giảm tắc nghẽn và đẩy nhanh tiến độ.


#### Mẫu hạ tầng và thực tiễn chuẩn hóa

- **Template IaC với Terraform**: Bao gồm code mẫu và quy trình thực tế đã chuyển sang EMR Serverless, giúp các nhóm triển khai nhanh chóng.


- **Giải pháp truy cập đa tài khoản**: Vì môi trường hoạt động trải dài trên nhiều tài khoản AWS, họ xây dựng module hướng dẫn từng bước cấu hình quyền Assume Role. Một kho riêng được tạo ra để các nhóm định nghĩa và tự động hoá việc tạo vai trò và chính sách truy cập.


#### Tích hợp Airflow

Airflow là công cụ điều phối chính của AppsFlyer, nên việc giữ nguyên trải nghiệm người dùng là rất quan trọng.

Họ xây dựng một operator Airflow riêng cho Spark trên EMR Serverless, tương thích với operator cũ. Một package Python chứa operator này được triển khai trên toàn bộ cụm Airflow, giúp giảm thiểu thay đổi code và đảm bảo sự chuyển đổi liền mạch.

#### Xử lý các vấn đề phân quyền phổ biến

- **Tài liệu phân quyền chi tiết**: Bao gồm hướng dẫn thiết lập quyền cho Athena, BigQuery, Kafka, Vault, GIT, v.v.


- **Cấu hình mặc định cho Spark**: Bao gồm các giải pháp sẵn có để thu thập lineage từ job Spark chạy trên EMR Serverless, đảm bảo truy xuất nguồn gốc.


#### Đồng hành cùng các nhóm R&D

- **Họp định kỳ hàng tuần**: Các nhóm cập nhật tiến độ, nêu rõ vướng mắc, thảo luận và cam kết.


- **Hỗ trợ chủ động**: DataInfra giúp xử lý các vấn đề ngay trong cuộc họp để tránh trì hoãn.



## 5. CI/CD cho Spark job
### Quản lý và triển khai mã nguồn ứng dụng Spark

Với các kỹ sư dữ liệu tại AppsFlyer, việc phát triển và triển khai ứng dụng Spark là một nhiệm vụ cốt lõi mỗi ngày. Nhóm Nền tảng Dữ liệu (Data Platform) tập trung vào việc tìm kiếm và triển khai các công cụ cũng như cơ chế bảo vệ phù hợp, không chỉ giúp đơn giản hóa quá trình di chuyển sang EMR Serverless mà còn tối ưu hóa vận hành lâu dài.

Có hai phương pháp chính để chạy mã Spark trên EMR Serverless:

1. **Sử dụng ảnh container tùy chỉnh (custom container images)**


2. **Sử dụng file JAR hoặc Python**


Ban đầu, ảnh container tùy chỉnh được xem là lựa chọn tiềm năng nhờ khả năng tùy biến cao hơn so với JAR – điều này có thể giúp nhóm DataInfra dễ dàng chuyển đổi các khối lượng công việc đang chạy. Tuy nhiên, sau khi nghiên cứu sâu hơn, họ nhận ra rằng dù ảnh container rất mạnh mẽ, nhưng lại đi kèm những chi phí không nhỏ, đặc biệt khi triển khai ở quy mô lớn.

Những thách thức của ảnh container bao gồm:

- Chỉ được hỗ trợ từ phiên bản EMR 6.9.0 trở lên, trong khi một số workload của AppsFlyer vẫn còn sử dụng phiên bản cũ hơn.


- Tài nguyên của EMR Serverless sẽ bị tính phí ngay từ lúc bắt đầu tải ảnh container cho đến khi các worker dừng lại – bao gồm CPU, bộ nhớ và dung lượng lưu trữ.


- Việc sử dụng ảnh container đòi hỏi phải thiết lập hệ thống CI/CD khác biệt so với việc chỉ biên dịch JAR hoặc file Python, kéo theo nhiều công việc vận hành không cần thiết.


Sau cùng, AppsFlyer quyết định ưu tiên hoàn toàn cho file JAR, chỉ dùng custom image trong những trường hợp thật sự đặc biệt đòi hỏi mức tùy chỉnh cao. Họ kết luận rằng việc không sử dụng custom image vẫn đáp ứng tốt cho phần lớn nhu cầu của mình.



### Góc nhìn CI/CD (Tích hợp và Triển khai liên tục)

Từ góc độ CI/CD, nhóm DataInfra của AppsFlyer định hướng đồng bộ với triết lý GitOps của toàn công ty – đảm bảo rằng cả hạ tầng lẫn mã ứng dụng đều được kiểm soát phiên bản, xây dựng và triển khai hoàn toàn qua Git.

![alt text](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/GitFlow.png)



### Tích hợp liên tục với JAR

Trong quá trình xây dựng các gói ứng dụng (application artifacts), AppsFlyer đã thử nghiệm nhiều phương án. Một số yếu tố then chốt được đưa vào cân nhắc gồm:

- Sử dụng Amazon S3 làm nơi lưu trữ gốc cho file JAR trên EMR Serverless


- Hỗ trợ nhiều phiên bản khác nhau cho cùng một job


- Phân tách môi trường staging và production


- Hỗ trợ vá lỗi nhanh (hotfix), cập nhật bản vá và khả năng rollback dễ dàng


Ban đầu, họ dùng kho lưu trữ package riêng nhưng gặp khó khăn vì phải tự phát triển cơ chế chuyển giao file JAR vào S3 hoặc thiết lập khả năng runtime để truy xuất mã từ nguồn ngoài.

Khi chuyển sang dùng trực tiếp Amazon S3, họ tiếp tục đánh giá nhiều cách tiếp cận khác nhau:

- **Bucket**: Dùng một bucket chung hay tách riêng staging và production?


- **Versioning**: Dùng cơ chế version có sẵn của S3 hay tải lên file mới mỗi lần?


- **Hotfix**: Ghi đè lên file JAR hiện tại hay tạo phiên bản mới?


Cuối cùng, họ chọn hướng build bất biến (immutable builds) để đảm bảo tính đồng nhất khi triển khai trên các môi trường khác nhau.

Mỗi kho mã Spark khi đẩy code lên nhánh chính sẽ kích hoạt quy trình CI để:

- Kiểm tra và gán version theo chuẩn SemVer (major.minor.patch),


- Biên dịch thành JAR,


- Tải JAR lên S3 với ba đường dẫn khác nhau, gắn tag version cho từng file:


```
<BucketName>/<SparkJobName>/<major>.<minor>.<patch>/app.jar  
<BucketName>/<SparkJobName>/<major>.<minor>/app.jar  
<BucketName>/<SparkJobName>/<major>/app.jar
```




Cách làm này giúp AppsFlyer kiểm soát cực kỳ chi tiết: một số job có thể chạy phiên bản mới nhất của major version, trong khi các job yêu cầu ổn định cao hoặc có SLA khắt khe có thể "khóa cứng" vào một patch version cụ thể.



### Triển khai liên tục với EMR Serverless

Sau khi file JAR được tải lên Amazon S3, quy trình CI hoàn tất và một quy trình CD riêng sẽ bắt đầu.

Quy trình CD được thực hiện bằng cách cập nhật mã hạ tầng (dùng Terraform), trỏ đến file JAR mới đã tải lên. Sau đó, ứng dụng staging hoặc production sẽ sử dụng phiên bản mã mới này – đánh dấu quá trình triển khai hoàn tất.



### Khả năng rollback ứng dụng Spark

Khi cần rollback, AppsFlyer chỉ cần cập nhật lại cấu hình IaC của job Spark, trỏ về phiên bản JAR ổn định trước đó trong Amazon S3.

AppsFlyer tin rằng mọi quy trình tự động tác động đến môi trường production – như CD – đều cần có cơ chế "phá kính khẩn cấp" (breaking glass) để xử lý sự cố. Trong trường hợp khẩn cấp, họ có thể ghi đè thủ công file JAR trong S3, đồng thời vẫn sử dụng versioning của S3 để dễ kiểm soát và theo dõi.



### Ứng dụng đơn vs. Ứng dụng đa job

Khi dùng EMR Serverless, một quyết định kiến trúc quan trọng là:
 **Nên tạo một ứng dụng riêng cho mỗi Spark job hay dùng một ứng dụng chung tự mở rộng để chạy nhiều job?**

Bảng sau tóm tắt các yếu tố cần cân nhắc:
| **Khía cạnh**          | **Ứng dụng đơn cho mỗi job**              | **Ứng dụng chung cho nhiều job**                      |
|------------------------|-------------------------------------------|--------------------------------------------------------|
| **Tính chất logic**     | Mỗi job là một ứng dụng độc lập           | Nhiều job chia sẻ một ứng dụng chung                  |
| **Cấu hình chia sẻ**    | Rất hạn chế – mỗi job tự cấu hình         | Có thể chia sẻ qua `spark-defaults`                   |
| **Cách ly tài nguyên**  | Cách ly tối đa                            | Cách ly thông qua IAM role từng job                   |
| **Tính linh hoạt**      | Cao – dễ tùy chỉnh từng job               | Giảm overhead nhờ tái sử dụng cấu hình                |
| **Chi phí vận hành**    | Tốn công quản lý do nhiều ứng dụng        | Ít hơn, nhưng phải kiểm soát tranh chấp tài nguyên   |
| **Trường hợp sử dụng**  | Job đặc thù, yêu cầu tách biệt            | Các workload liên quan, hưởng lợi từ chia sẻ cấu hình |

AppsFlyer đã cân bằng các yếu tố trên để xây dựng mô hình EMR Serverless phù hợp, đáp ứng đa dạng nhu cầu xử lý Spark của các nhóm trong tổ chức.


## 6. Operator Airflow tùy chỉnh
### Operator Airflow: Đơn giản hóa chuyển đổi sang EMR Serverless

Trước khi chuyển đổi, các nhóm tại AppsFlyer sử dụng một Spark operator tùy chỉnh do nhóm DataInfra phát triển, tích hợp trực tiếp vào môi trường Airflow dưới dạng thư viện Python – và đây là thành phần thiết yếu trong hệ thống xử lý dữ liệu.

Operator này hỗ trợ nhiều tính năng:

- **Retry và cảnh báo**: logic retry tích hợp sẵn, kết nối PagerDuty để thông báo


- **Truy xuất quyền AWS tự động**: dựa vào tên IAM role


- **Cấu hình mặc định tùy chỉnh**: cho từng job


- **Quản lý trạng thái job**: theo dõi, kiểm soát tiến trình job Spark


Khi chuyển sang EMR Serverless, thay vì thay đổi toàn bộ cách làm, nhóm DataInfra quyết định xây dựng một operator mới, mô phỏng theo Spark operator cũ để:

- **Giữ nguyên giao diện quen thuộc**


- **Tận dụng lại các DAG hiện có**


- **Không cần chỉnh sửa hàng loạt code**


Operator mới được đóng gói dưới dạng thư viện Python, có thể cài đặt sẵn trên các cụm Airflow. Nhờ vậy, các nhóm có thể tiếp tục sử dụng theo đúng cách họ từng làm trước đây.

Mục tiêu của operator tùy chỉnh này:

- **Chuyển đổi mượt mà**: giữ gần như nguyên vẹn giao diện và hành vi operator Spark cũ trên Hadoop


- **Tính năng đầy đủ**: bổ sung các tính năng còn thiếu ở operator gốc như:
  + Logic retry tích hợp
  + Kết nối PagerDuty
  + Tự động lấy quyền IAM
  + Cấu hình Spark mặc định và hỗ trợ package


- **Dễ tích hợp**: sử dụng đơn giản như operator cũ


Operator này giúp ẩn đi những phức tạp phía sau khi submit job lên EMR Serverless, đồng thời tuân theo các thực tiễn nội bộ và bổ sung các tính năng thiết yếu cho môi trường production.

Dưới đây là ví dụ về cách sử dụng *SparkBatchJobEmrServerlessOperator* trong một DAG của Airflow:

```python
return SparkBatchJobEmrServerlessOperator(
    task_id=task_id,  # Tên duy nhất cho task trong DAG

    jar_file=jar_file,  # Đường dẫn đến file JAR của Spark job trên S3
    main_class="<main class path>",

    spark_conf=spark_conf,

    app_id=default_args["<emr_serverless_application_id>"],  # ID ứng dụng EMR Serverless
    execution_role=default_args["<job_execution_role_arn>"],  # IAM Role để chạy job

    polling_interval_sec=120,  # Tần suất kiểm tra trạng thái job
    execution_timeout=timedelta(hours=1),  # Thời gian tối đa cho phép chạy

    retries=5,  # Số lần retry nếu job thất bại
    app_args=[],  # Tham số truyền vào Spark job

    depends_on_past=True,  # Đảm bảo task này chỉ chạy sau khi task trước đó hoàn tất

    tags={'owner': '<team_tag>'},  # Gắn thẻ metadata
    aws_assume_role="<my_aws_role>",  # Role sử dụng để truy cập chéo tài khoản

    alerting_policy=ALERT_POLICY_CRITICAL.with_slack_channel(sc),  # Thiết lập cảnh báo
    owner="<team_owner>",

    dag=dag  # DAG chứa task này
)
```





## 7. Quản lý phân quyền đa tài khoản
### Truy cập chéo tài khoản AWS: Đơn giản hóa quy trình EMR

AppsFlyer vận hành trên nhiều tài khoản AWS, nên cần một cơ chế truy cập chéo tài khoản (cross-account) vừa an toàn vừa linh hoạt. Cụ thể, các job EMR Serverless được khởi chạy trong tài khoản production, trong khi dữ liệu lại nằm ở một tài khoản data riêng biệt.

Để đảm bảo các job này vẫn truy cập được dữ liệu cần thiết, họ dùng Assume Role — một tính năng cho phép vai trò (role) ở tài khoản này được “nhận diện” và sử dụng vai trò khác ở tài khoản kia một cách có kiểm soát.

![alt text](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/trust-relationship.png)


#### Kiến trúc phân quyền chéo tài khoản của AppsFlyer

Mô hình được chia thành 3 role chính:

- **EMR Role** – Quản lý việc chạy ứng dụng EMR Serverless trong tài khoản production. Role này được gắn trực tiếp vào các worker của Airflow để DAG có thể truy cập.


- **Execution Role** – Được sử dụng bởi Spark job trên EMR Serverless. Role này được EMR Role truyền vào DAG code.


- **Data Role** – Nằm trong tài khoản chứa dữ liệu, cho phép Execution Role truy cập S3 và các dịch vụ AWS khác. Chỉ những role có tag đúng team mới được assume.


Tất cả các role và policy đều được gắn thẻ (tag) theo team, đảm bảo chỉ truy cập được dữ liệu và tài nguyên đúng phạm vi.



### Tự động hóa và đơn giản hóa việc cấp quyền

AppsFlyer đã thiết kế một quy trình chuẩn giúp các nhóm dễ dàng chuyển sang EMR Serverless mà không phải lo về cấp quyền:

1. EMR Role tích hợp sẵn trong worker của Airflow, cho phép mọi DAG trong cụm Airflow của từng team đều dùng được role này:

```json
{
   "Version": "2012-10-17",
   "Statement": [
      {
         "Effect": "Allow",
         "Action": "iam:PassRole",
         "Resource": "arn:aws:iam::account-id:role/execution-role",
         "Condition": {
            "StringEquals": {
               "iam:ResourceTag/Team": "team-tag"
            }
         }
      }
   ]
}
```




2. EMR Role trong DAG được cấu hình để chuyển tiếp quyền cho Execution Role:


```json
{

  "Version": "2012-10-17",

  "Statement": [

    {

      "Effect": "Allow",

      "Action": "sts:AssumeRole",

      "Resource": "arn:aws:iam::data-account-id:role/data-role",

      "Condition": {

        "StringEquals": {

          "iam:ResourceTag/Team": "team-tag"

        }

      }

    }

  ]

}
```


3. Execution Role sau đó tự động assume Data Role khi job thực thi:


```json
{

  "Version": "2012-10-17",

  "Statement": [

    {

      "Effect": "Allow",

      "Principal": {

        "AWS": "arn:aws:iam::production-account-id:role/execution-role"

      },

      "Action": "sts:AssumeRole"

    }

  ]

}
```


4. Mọi cấu hình role, policy và trust relationship đều được quản lý trong GitLab repo riêng biệt. Việc tạo, cập nhật được tự động hóa hoàn toàn qua pipeline CI/CD của GitLab, đảm bảo sự đồng nhất và hạn chế lỗi thủ công.



### Lợi ích từ mô hình phân quyền này

- **Truy cập liền mạch** – Các team không cần thao tác gì về quyền truy cập, vì mọi thứ đã được cấu hình sẵn.


- **Bảo mật và mở rộng tốt** – Dựa trên vai trò và tag, hệ thống đảm bảo đúng người đúng quyền. Khi thêm team hoặc tài khoản mới, không cần viết lại policy.


- **Tự động hóa cao** – CI/CD giúp triển khai role và policy nhanh chóng, hạn chế sai sót, dễ theo dõi thay đổi và quản lý phiên bản.


- **Linh hoạt cho từng team** – Mỗi team có thể dùng operator EMR riêng hoặc mặc định mà vẫn truy cập được dữ liệu an toàn.


Cách tiếp cận này giúp AppsFlyer đơn giản hóa việc vận hành trong môi trường AWS phức tạp, cho phép các nhóm kỹ thuật tập trung vào công việc chính – thay vì loay hoay với hệ thống phân quyền.


## 8. Tích hợp Lineage với Spline
### Tích hợp Lineage vào EMR Serverless

Để tăng cường khả năng quan sát dòng dữ liệu (data lineage) trong toàn hệ thống, AppsFlyer phát triển một giải pháp thu thập lineage chi tiết đến cấp độ cột. Dữ liệu lineage này được lưu trữ trên Amazon S3 và đưa vào hệ thống quản lý metadata nội bộ có tên là DataHub.

Hiện tại, AppsFlyer đang thu thập lineage cấp cột từ nhiều nguồn: Athena, BigQuery, Spark,...

Phần này tập trung vào cách thu thập lineage từ Spark jobs trong EMR Serverless.



#### Thu thập Lineage Spark bằng Spline

Để thu thập lineage, AppsFlyer sử dụng Spline – một công cụ mã nguồn mở chuyên để ghi lại các hoạt động và luồng dữ liệu trong Spark.

Họ đã tinh chỉnh hành vi mặc định của Spline để tạo ra một “Spline object” tùy biến, phù hợp với yêu cầu nội bộ.

- Trước khi chuyển sang EMR Serverless:
 Họ inject Spline agent vào job thông qua operator Spark tùy chỉnh trong Airflow.


- Sau khi chuyển đổi:
 Spline được tích hợp trực tiếp vào ứng dụng Spark chạy trên EMR Serverless.


Quy trình hoạt động của hệ thống này như sau:

1. Khi job Spark chạy, Spline ghi lại toàn bộ metadata liên quan đến truy vấn và các phép biến đổi dữ liệu.


2. Metadata được xuất ra thành file Spline object và lưu vào một bucket riêng trên Amazon S3.


3. Sau đó, dữ liệu này được xử lý và chuyển thành các object thể hiện lineage cấp cột – phù hợp với kiến trúc dữ liệu của AppsFlyer.


4. Cuối cùng, dữ liệu được ingest vào DataHub – giúp các nhóm dễ dàng theo dõi quan hệ phụ thuộc giữa các bảng, truy vấn và pipeline trong toàn hệ thống.

Dưới đây là hình ảnh minh họa cho sơ đồ lineage (phụ thuộc dữ liệu) được trích xuất từ DataHub:

![alt text](![alt text](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/flow-diagram.png)

### Những thách thức và cách AppsFlyer giải quyết

#### Các thách thức gặp phải:

- **Hỗ trợ nhiều phiên bản ứng dụng EMR Serverless** – Mỗi ứng dụng EMR có thể yêu cầu phiên bản Spark và Scala khác nhau.


- **Đa dạng cách dùng operator** – Các nhóm sử dụng cả operator tự viết lẫn operator mặc định của EMR Serverless, khiến việc tích hợp Spline không đồng nhất.


- **Đảm bảo toàn bộ hệ thống đều có Spline** – Cần đảm bảo mọi Spark job, dù chạy ở bất kỳ tài khoản nào, đều có Spline để thu thập lineage.


#### Cách AppsFlyer giải quyết:

- **Spline agent theo phiên bản** – Họ tạo Spline agent riêng tương ứng với từng phiên bản EMR Serverless (ví dụ: ứng dụng dùng EMR 7.0.1 sẽ dùng Spline.7.0.1), đảm bảo tương thích tuyệt đối.


- **Tích hợp Spline vào Spark defaults** – Spline được gắn trực tiếp vào cấu hình mặc định của Spark, nên mọi Spark job đều tự động sử dụng mà không cần sửa từng job riêng lẻ.


- **Tự động kiểm tra cấu hình** – Một quy trình kiểm tra tự động sẽ:


 1. Phát hiện khi có ứng dụng EMR Serverless mới được tạo.


 2. Kiểm tra xem Spline đã được cấu hình đúng trong Spark defaults hay chưa.


 3. Gửi cảnh báo PagerDuty nếu phát hiện sai sót.




### Ví dụ tích hợp bằng Terraform

AppsFlyer dùng Terraform cùng local-exec để cấu hình Spark defaults cho từng ứng dụng EMR. Nhờ đó, Spline agent luôn được áp dụng một cách tự động, không cần chỉnh sửa ở job hay Airflow operator.

#### Lợi ích của việc tích hợp Spline:

- **Quan sát toàn diện** – Tự động thu thập lineage giúp dễ theo dõi dòng chảy và biến đổi dữ liệu.

- **Mở rộng linh hoạt** – Dùng Spline theo từng phiên bản EMR Serverless giúp đảm bảo tính tương thích.

- **Kiểm soát chủ động** – Các công cụ tự kiểm tra đảm bảo tính nhất quán và đầy đủ của hệ thống lineage.

- **Tuân thủ và truy vết** – Lineage được ingest vào DataHub giúp kiểm toán, truy vết và hiểu mối liên kết dữ liệu dễ dàng hơn.




## 9. Giám sát và observability
### Giám sát và khả năng quan sát (Observability)

Khi thực hiện di chuyển quy mô lớn sang EMR Serverless, giám sát và observability là yếu tố then chốt để đảm bảo độ ổn định, dễ debug và kiểm soát chi phí.

### Các KPI mà nhóm DataInfra của AppsFlyer theo dõi:

1. **Giám sát hạ tầng (infrastructure-level)**:
   - Tài nguyên EMR Serverless (bao gồm chi phí)
   - Lượt sử dụng API EMR Serverless

2. **Giám sát ứng dụng Spark (application-level)**:
   - stdout / stderr logs
   - Chỉ số từ Spark engine

3. **Tập trung hóa observability qua Datadog**

   **Các chỉ số chính**:

   - **Service quota**:
     - Theo dõi vCPU, API usage...

   - **Trạng thái job**:
     - Số lượng job đang chạy, thành công, thất bại, pending, hủy

   - **Giới hạn tài nguyên**:
     - Giám sát vCPU, RAM, storage sử dụng so với tối đa cho phép

   - **Chi tiết cấp worker**:
     - Phân tích sử dụng CPU, RAM, bộ nhớ tạm của từng worker

   - **Theo dõi phân bổ tài nguyên**:
     - Phân biệt loại tài nguyên theo nhu cầu (OnDemand) hay khởi tạo trước (PreInit)

   - **Tỉ lệ thành công của job theo thời gian**

![Alt Text](https://d2908q01vomqb2.cloudfront.net/b6692ea5df920cad691c20319a6fffd7a4a766b8/2025/05/05/charts.png)




### Quản lý log: đơn giản hóa việc chuyển log từ EMR Serverless đến Datadog

Ban đầu, AppsFlyer thử nghiệm nhiều cách như:

- **S3 + Lambda** chuyển log vào Datadog


- **CloudWatch + Kinesis + Firehose**


- **Log4j shipper** gửi log trực tiếp từ Spark


**Nhưng cuối cùng họ chọn một hướng gọn hơn:**

Phát triển một **plugin Spark tùy chỉnh**, xuất log thẳng từ EMR Serverless đến Datadog — **không cần Lambda, không cần extra infra**.



### Plugin log tùy chỉnh của AppsFlyer: Hoạt động thế nào?

**Các khả năng chính:**

- Xuất log tự động từ EMR Serverless → Datadog
- Không cần Lambda hay shipper ngoài
- Quản lý API key bằng Vault (không hardcode)
- Hỗ trợ log4j tùy chỉnh và đa cấp độ log
- Chạy được cả trên driver lẫn executor của Spark

**Thành phần chính:**

1. **Driver plugin – cấu hình log tại Spark driver:**

    ```python
    initialize() {
      nếu user cung cấp log4j.xml thì dùng,
      ngược lại:
        - lấy metadata job từ EMR
        - gọi Vault để lấy Datadog API key
        - áp dụng config log mặc định
    }
    ```

2. **Executor plugin – đảm bảo các executor dùng cấu hình giống driver:**

    ```python
    initialize() {
      lấy cấu hình từ driver,
      áp dụng cài đặt log (log4j, log level)
    }
    ```

3. **Plugin chính:**

    ```python
    registerPlugin() {
      return (driverPlugin, executorPlugin);
    }
    ```

4. **Xác thực Vault – thông qua IAM để lấy API key:**

    ```python
    loginToVault(role, vaultAddress) {
      tạo yêu cầu AWS ký tên,
      xác thực với Vault,
      nhận token
    }

    getDatadogApiKey(token, path) {
      truy vấn Vault để lấy key
    }
    ```

---

### Cách thiết lập plugin

1. **Thêm dependency vào dự án:**

```xml
<dependency>
  <groupId>com.AppsFlyer.datacom</groupId>
  <artifactId>emr-serverless-logger-plugin</artifactId>
  <version><!-- chèn version ở đây --></version>
</dependency>
```



2. **Bật plugin trong Spark:**

```bash
--conf "spark.plugins=com.AppsFlyer.datacom.emr.plugin.LoggerSparkPlugin"
--conf "spark.datacom.emr.plugin.vaultAuthRole=your_vault_role"
```



3. **Dùng log4j config tùy chỉnh (hoặc mặc định):**

```bash
--conf "spark.datacom.emr.plugin.location=classpath:my_custom_log4j.xml"
```


4. **Thiết lập log level qua biến môi trường:**

```bash
--conf "spark.emr-serverless.driverEnv.ROOT_LOG_LEVEL=WARN"

--conf "spark.executorEnv.ROOT_LOG_LEVEL=WARN"

--conf "spark.emr-serverless.driverEnv.LOG_LEVEL=DEBUG"

--conf "spark.executorEnv.LOG_LEVEL=DEBUG"
```


5. **Cấu hình Vault và xác thực key Datadog**




## 10. Tổng kết

Chuyển đổi sang EMR Serverless đã mang lại cho AppsFlyer:

- **Tự chủ và linh hoạt cao** – Các nhóm không còn phụ thuộc vào team hạ tầng trung tâm.


- **Giảm sự cố spot instance** – Tránh được gián đoạn thường gặp khi dùng Hadoop tự quản.


- **Tăng độ ổn định và hiệu quả** – Với khả năng scale tự động, nhóm chỉ tập trung vào xử lý dữ liệu thay vì lo hạ tầng.


- **Quan sát tốt hơn** – Từ log đến lineage, mọi thứ được kiểm soát rõ ràng.


Ruli Weisbach – EVP R&D tại AppsFlyer nhận xét:

“EMR Serverless thật sự là bước ngoặt lớn cho chúng tôi – tiết kiệm chi phí đáng kể, giảm thiểu công việc quản trị và tăng khả năng co giãn đến mức tối đa.”
## 📖 Tài liệu tham khảo
### Tài liệu tham khảo nếu bạn muốn làm tương tự:

- [Amazon EMR Serverless User Guide](https://docs.aws.amazon.com/emr/latest/EMR-Serverless-UserGuide/emr-serverless.html)
- [EMR Serverless cost estimator](https://aws.amazon.com/blogs/big-data/amazon-emr-serverless-cost-estimator/)
- [Chạy các workload tương tác từ EMR Studio](https://aws.amazon.com/blogs/big-data/run-interactive-workloads-on-amazon-emr-serverless-from-amazon-emr-studio/)
- [GoDaddy: tiết kiệm 60% chi phí, tăng 50% hiệu năng khi dùng EMR Serverless](https://aws.amazon.com/blogs/big-data/how-the-godaddy-data-platform-achieved-over-60-cost-reduction-and-50-performance-boost-by-adopting-amazon-emr-serverless/)



### Tác giả:

- **Roy Ninio** – Trưởng nhóm AI Platform tại AppsFlyer


- **Avichay Marciano** – Sr. Analytics Solutions Architect tại AWS


- **Eitav Arditti** – AWS Sr. Solutions Architect, chuyên về serverless và edge


- **Yonatan Dolan** – Principal Analytics Specialist tại AWS, chuyên Apache Iceberg và lakehouse








---

*© 2025 – Bản dịch thuộc về Nguyễn Thành Đạt. Vui lòng dẫn nguồn khi chia sẻ.*
