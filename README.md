## Tablespace_check
* 위 Repository는 Database의 Tablespace 사용량을 확인할 수 있는 Script로 구성 (최초 ~ 최신버전)
* Python과 Aws SNS Service, Slack SNS Service를 이용하여 개발
* 기존의 수동으로 모니터링하던 작업을 매일 30분마다 Scheduling 작업을 걸어서 Tablespace 사용량이 95%이상
되었을 때 SNS Service를 이용해 메신저로 확인할 수 있게 자동화 완료


### Tablespace File comment
|  구분| 파일명| Comments|
|:-----: |----------------------------------:|---------------------------------:|
| Python | aws_sns_alert_TBS_size_chk_v2.1.py| 현재 운영상에 적용되고 있는 Script |
| Python | tablespace_size_chk.py| 최초 개발한 Script |
| Python | tablespace_size_chk2.py| 이전버전에서 scale_up & out 내용 추가 |
| Jupyter| tablespace_size_chk2.ipynb| tablespace_size_chk2.py Script를 시각화 |
| Python | TBS_size_chk_v1.py| tablespace_size_chk2의 scale_up 수정 |
| Python | TBS_size_chk_v2.py| TBS_size_chk_v1 Script 정리 |
