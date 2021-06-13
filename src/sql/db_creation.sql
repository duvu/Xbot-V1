create table tbl_company
(
    Code         varchar(32)  not null
        primary key,
    CatID        int          null,
    Exchange     varchar(256) null,
    ID           int          null,
    IndustryName varchar(256) null,
    Name         varchar(256) null,
    TotalShares  bigint       null,
    URL          varchar(512) null
);

create table tbl_finance_info
(
    company_id             int         not null,
    year_period            int         not null,
    quarter_period         varchar(32) not null,
    audited_status         varchar(32) null,
    code                   varchar(32) null,
    eps                    double      null,
    bvps                   double      null,
    pe                     double      null,
    ros                    double      null,
    roea                   double      null,
    roaa                   double      null,
    current_assets         double      null,
    total_assets           double      null,
    liabilities            double      null,
    short_term_liabilities double      null,
    owner_equity           double      null,
    minority_interest      double      null,
    net_revenue            double      null,
    gross_profit           double      null,
    operating_profit       double      null,
    profit_after_tax       double      null,
    net_profit             double      null,
    primary key (company_id, year_period, quarter_period)
);

create table tbl_finance_info_ctkh
(
    company_id          int         not null,
    year_period         int         not null,
    code                varchar(32) not null,
    total_revenue       double      null,
    net_revenue         double      null,
    profit_after_tax    double      null,
    profit_before_tax   double      null,
    cash_divident_rate  double      null,
    stock_divident_rate double      null,
    divident_rate       double      null,
    primary key (company_id, year_period, code)
);

create table tbl_mentions
(
    id              int auto_increment
        primary key,
    symbol          varchar(32)   null,
    mentioned_at    datetime      null,
    mentioned_by_id varchar(64)   null,
    mentioned_by    varchar(255)  null,
    mentioned_in    varchar(255)  null comment 'Discord or Internet',
    message         varchar(1024) null
);

create table tbl_price_board_day
(
    code varchar(32) not null,
    t    int         not null,
    o    double      null,
    h    double      null,
    l    double      null,
    c    double      null,
    v    double      null,
    primary key (code, t)
);

create table tbl_price_board_minute
(
    t    int         not null,
    code varchar(32) not null,
    o    double      null,
    h    double      null,
    l    double      null,
    c    double      null,
    v    double      null,
    primary key (code, t)
);

