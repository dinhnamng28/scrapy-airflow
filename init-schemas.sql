-- Kết nối tới cơ sở dữ liệu mặc định
\c da_postgres;

-- Tạo các database
CREATE DATABASE jobsgo
WITH 
    ENCODING 'UTF8'
    LC_COLLATE 'vi_VN.UTF-8'
    LC_CTYPE 'vi_VN.UTF-8'
    TEMPLATE template0;

CREATE DATABASE vnwork
WITH 
    ENCODING 'UTF8'
    LC_COLLATE 'vi_VN.UTF-8'
    LC_CTYPE 'vi_VN.UTF-8'
    TEMPLATE template0;

-- CREATE DATABASE career
-- WITH 
--     ENCODING 'UTF8'
--     LC_COLLATE 'vi_VN.UTF-8'
--     LC_CTYPE 'vi_VN.UTF-8'
--     TEMPLATE template0;



