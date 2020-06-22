
create table ANALYSIS_CONF_TAB
(
	SYS_CODE VARCHAR(20) not null,
	ORI_TABLE_CODE VARCHAR(128) not null,
	TRANS_TABLE_CODE VARCHAR(128),
	ETL_DATE CHAR(8),
	DATE_OFFSET VARCHAR(4),
	FEATURE_FLAG CHAR(1) not null,
	FD_FLAG CHAR(1) not null,
	PK_FLAG CHAR(1) not null,
	FK_FLAG CHAR(1) not null,
	FD_CHECK_FLAG CHAR(1) not null,
	DIM_FLAG CHAR(1) not null,
	INCRE_TO_FULL_FLAG CHAR(1) not null,
	ANA_ALG VARCHAR(4) not null,
	FD_SAMPLE_COUNT VARCHAR(8),
	TO_ANA_TB_PK VARCHAR(128),
	FK_ANA_MODE VARCHAR(32),
	JOINT_FK_FLAG CHAR(1) not null,
	JOINT_FK_ANA_MODE VARCHAR(32),
	constraint "P_Key_1"
		primary key (SYS_CODE, ORI_TABLE_CODE)
)
;

comment on table ANALYSIS_CONF_TAB is '数据分析配置表'
;

comment on column ANALYSIS_CONF_TAB.SYS_CODE is '系统代码'
;

comment on column ANALYSIS_CONF_TAB.ORI_TABLE_CODE is '原始表编号'
;

comment on column ANALYSIS_CONF_TAB.TRANS_TABLE_CODE is '转换后表编号'
;

comment on column ANALYSIS_CONF_TAB.ETL_DATE is '数据时间'
;

comment on column ANALYSIS_CONF_TAB.DATE_OFFSET is '时间偏移量'
;

comment on column ANALYSIS_CONF_TAB.FEATURE_FLAG is '码值：
1-是；
0-否。
'
;

comment on column ANALYSIS_CONF_TAB.FD_FLAG is '码值：
1-是；
0-否。'
;

comment on column ANALYSIS_CONF_TAB.PK_FLAG is '码值：
1-是；
0-否。'
;

comment on column ANALYSIS_CONF_TAB.FK_FLAG is '码值：
1-是；
0-否。'
;

comment on column ANALYSIS_CONF_TAB.FD_CHECK_FLAG is '码值：
1-是；
0-否。'
;

comment on column ANALYSIS_CONF_TAB.DIM_FLAG is '码值：
1-是；
0-否。'
;

comment on column ANALYSIS_CONF_TAB.INCRE_TO_FULL_FLAG is '码值：
1-是；
0-否。'
;

comment on column ANALYSIS_CONF_TAB.ANA_ALG is '码值：
1-是；
0-否。'
;

comment on column ANALYSIS_CONF_TAB.FD_SAMPLE_COUNT is '码值：
1-是；
0-否。'
;

comment on column ANALYSIS_CONF_TAB.TO_ANA_TB_PK is '待分析表主键'
;

comment on column ANALYSIS_CONF_TAB.FK_ANA_MODE is '外键分析模式'
;

comment on column ANALYSIS_CONF_TAB.JOINT_FK_FLAG is '是否进行联合外键分析
1是0否'
;

comment on column ANALYSIS_CONF_TAB.JOINT_FK_ANA_MODE is 'all'
;

create table ANALYSIS_SCHEDULE_TAB
(
	SYS_CODE VARCHAR(20) not null,
	ORI_TABLE_CODE VARCHAR(128) not null,
	FEATURE_SCHE CHAR(1),
	FEATURE_START_DATE TIMESTAMP(6),
	FEATURE_END_DATE TIMESTAMP(6),
	FD_SCHE CHAR(1),
	FD_START_DATE TIMESTAMP(6),
	FD_END_DATE TIMESTAMP(6),
	PK_SCHE CHAR(1),
	PK_START_DATE TIMESTAMP(6),
	PK_END_DATE TIMESTAMP(6),
	FK_SCHE CHAR(1),
	FK_START_DATE TIMESTAMP(6),
	FK_END_DATE TIMESTAMP(6),
	FD_CHECK_SCHE CHAR(1),
	FD_CHECK_START_DATE TIMESTAMP(6),
	FD_CHECK_END_DATE TIMESTAMP(6),
	DIM_SCHE CHAR(1),
	DIM_START_DATE TIMESTAMP(6),
	DIM_END_DATE TIMESTAMP(6),
	INCRE_TO_FULL_SCHE CHAR(1),
	INCRE_TO_FULL_START_DATE TIMESTAMP(6),
	INCRE_TO_FULL_END_DATE TIMESTAMP(6),
	JOINT_FK_SCHE CHAR(1),
	JOINT_FK_START_DATE TIMESTAMP(6),
	JOINT_FK_END_DATE TIMESTAMP(6),
	constraint "P_Key_1"
		primary key (SYS_CODE, ORI_TABLE_CODE)
)
;

comment on table ANALYSIS_SCHEDULE_TAB is '分析进度表'
;

comment on column ANALYSIS_SCHEDULE_TAB.SYS_CODE is '系统代码'
;

comment on column ANALYSIS_SCHEDULE_TAB.ORI_TABLE_CODE is '原始表编号'
;

comment on column ANALYSIS_SCHEDULE_TAB.FEATURE_SCHE is '字段特征分析进度'
;

comment on column ANALYSIS_SCHEDULE_TAB.FEATURE_START_DATE is '字段特征分析开始时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.FEATURE_END_DATE is '字段特征分析结束时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.FD_SCHE is '函数依赖分析进度
0：未完成
1：第一次函数依赖分析完成
2：第二次函数依赖分析完成'
;

comment on column ANALYSIS_SCHEDULE_TAB.FD_START_DATE is '函数依赖分析开始时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.FD_END_DATE is '函数依赖分析结束时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.PK_SCHE is '主键分析进度'
;

comment on column ANALYSIS_SCHEDULE_TAB.PK_START_DATE is '主键分析开始时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.PK_END_DATE is '主键分析结束时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.FK_SCHE is '外键分析进度'
;

comment on column ANALYSIS_SCHEDULE_TAB.FK_START_DATE is '外键分析开始时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.FK_END_DATE is '外键分析结束时间
函数依赖验证进度
函数依赖验证开始时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.FD_CHECK_SCHE is '函数依赖验证进度'
;

comment on column ANALYSIS_SCHEDULE_TAB.FD_CHECK_START_DATE is '函数依赖验证开始时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.FD_CHECK_END_DATE is '函数依赖验证结束时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.DIM_SCHE is '维度划分进度'
;

comment on column ANALYSIS_SCHEDULE_TAB.DIM_START_DATE is '维度划分进度开始时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.DIM_END_DATE is '维度划分进度结束时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.INCRE_TO_FULL_SCHE is '表转为全量进度'
;

comment on column ANALYSIS_SCHEDULE_TAB.INCRE_TO_FULL_START_DATE is '表转为全量开始时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.INCRE_TO_FULL_END_DATE is '表转为全量结束时间'
;

comment on column ANALYSIS_SCHEDULE_TAB.JOINT_FK_SCHE is '联合外键分析进度
0：未完成
1：已完成'
;

comment on column ANALYSIS_SCHEDULE_TAB.JOINT_FK_START_DATE is '联合外键分析开始时间
YYYY-MM-DD'
;

comment on column ANALYSIS_SCHEDULE_TAB.JOINT_FK_END_DATE is '联合外键分析结束时间
YYYY-MM-DD'
;

create table FEATURE_TAB
(
	SYS_CODE VARCHAR(20) not null,
	TABLE_SCHEMA VARCHAR(20) not null,
	TABLE_CODE VARCHAR(128) not null,
	COL_CODE VARCHAR(64) not null,
	COL_RECORDS DECIMAL(9) not null,
	COL_DISTINCT DECIMAL(9),
	MAX_LEN DECIMAL(9),
	MIN_LEN DECIMAL(9),
	AVG_LEN DECIMAL(9),
	SKEW_LEN DECIMAL(19,2),
	KURT_LEN DECIMAL(19,2),
	VAR_LEN DECIMAL(19,2),
	HAS_CHINESE CHAR(1),
	TECH_CATE CHAR(1),
	ST_TM TIMESTAMP(6) not null,
	END_TM TIMESTAMP(6),
	MEDIAN_LEN DECIMAL(9)
)
;

create table FIELD_CATE_RESULT
(
	SYS_CODE VARCHAR(20) not null,
	TABLE_CODE VARCHAR(128) not null,
	COL_CODE VARCHAR(64) not null,
	DIM_NODE VARCHAR(2048),
	ORIGIN_DIM VARCHAR(512),
	RELATION_TYPE VARCHAR(8),
	CATEGORY_SAME INTEGER,
	DIFF_FLG INTEGER,
	DIM_ORDER INTEGER,
	DEL_FLAG VARCHAR(2)
)
;

comment on table FIELD_CATE_RESULT is '维度划分结果表'
;

comment on column FIELD_CATE_RESULT.SYS_CODE is '系统代码'
;

comment on column FIELD_CATE_RESULT.TABLE_CODE is '原始表编号'
;

comment on column FIELD_CATE_RESULT.COL_CODE is '字段编号'
;

comment on column FIELD_CATE_RESULT.DIM_NODE is '所属维度节点编号'
;

comment on column FIELD_CATE_RESULT.ORIGIN_DIM is '原始所属维度节点编号'
;

comment on column FIELD_CATE_RESULT.RELATION_TYPE is '关系类型'
;

comment on column FIELD_CATE_RESULT.CATEGORY_SAME is '同维度类别编号'
;

comment on column FIELD_CATE_RESULT.DIFF_FLG is '区别标识'
;

comment on column FIELD_CATE_RESULT.DIM_ORDER is '同维度下同类别字段排序编号'
;

comment on column FIELD_CATE_RESULT.DEL_FLAG is '子集关系删除标识
1：不删除
0：删除'
;

create table FIELD_SAME_DETAIL
(
	LEFT_SYS_CODE VARCHAR(10),
	LEFT_TABLE_CODE VARCHAR(64),
	LEFT_COL_CODE VARCHAR(64),
	RIGHT_SYS_CODE VARCHAR(10),
	RIGHT_TABLE_CODE VARCHAR(64),
	RIGHT_COL_CODE VARCHAR(64),
	FK_ID VARCHAR(64),
	FK_TYPE CHAR(1),
	ANA_TIME TIMESTAMP(6),
	REL_TYPE VARCHAR(10)
)
;

comment on column FIELD_SAME_DETAIL.FK_ID is '关联的外键ID'
;

comment on column FIELD_SAME_DETAIL.FK_TYPE is '0：单一外键
1：联合外键'
;

comment on column FIELD_SAME_DETAIL.ANA_TIME is '分析时间'
;

comment on column FIELD_SAME_DETAIL.REL_TYPE is '关系类型，有同名(same)、相等(equals)、外键(fd)和互为外键(bfd)'
;

create table FIELD_SAME_RESULT
(
	SYS_CODE VARCHAR(20) not null,
	TABLE_CODE VARCHAR(128) not null,
	COL_CODE VARCHAR(64) not null,
	CATEGORY_SAME INTEGER,
	DIFF_FLG INTEGER,
	DIM_ORDER INTEGER,
	ANA_TIME TIMESTAMP(6)
)
;

comment on table FIELD_SAME_RESULT is '字段分组结果表'
;

comment on column FIELD_SAME_RESULT.SYS_CODE is '系统代码'
;

comment on column FIELD_SAME_RESULT.TABLE_CODE is '原始表编号'
;

comment on column FIELD_SAME_RESULT.COL_CODE is '字段编号'
;

comment on column FIELD_SAME_RESULT.CATEGORY_SAME is '同类别编号'
;

comment on column FIELD_SAME_RESULT.DIM_ORDER is '同类别字段排序编号'
;

comment on column FIELD_SAME_RESULT.ANA_TIME is '分析时间'
;

create table FK_INFO_TAB
(
	FK_SYS_CODE VARCHAR(20) not null,
	FK_NAME VARCHAR(1024) not null,
	FK_TABLE_OWNER VARCHAR(20) not null,
	FK_TABLE_CODE VARCHAR(128) not null,
	FK_COL_CODE VARCHAR(64) not null,
	SYS_CODE VARCHAR(64) not null,
	TABLE_SCHEMA VARCHAR(20) not null,
	TABLE_CODE VARCHAR(128) not null,
	COL_CODE VARCHAR(64) not null,
	ST_TM TIMESTAMP(6) not null,
	END_TM TIMESTAMP(6) not null,
	DATA_SRC VARCHAR(4),
	ID VARCHAR(64) not null
		constraint FK_INFO_TAB_ID_PK
			primary key
)
;

comment on table FK_INFO_TAB is '单一外键关系信息表'
;

comment on column FK_INFO_TAB.FK_SYS_CODE is '主表所在系统代码'
;

comment on column FK_INFO_TAB.FK_NAME is '外键名称'
;

comment on column FK_INFO_TAB.FK_TABLE_OWNER is '主表所属者'
;

comment on column FK_INFO_TAB.FK_TABLE_CODE is '主表表编号'
;

comment on column FK_INFO_TAB.FK_COL_CODE is '主键字段编号'
;

comment on column FK_INFO_TAB.SYS_CODE is '从表所在系统编码'
;

comment on column FK_INFO_TAB.TABLE_SCHEMA is '从表所在schema'
;

comment on column FK_INFO_TAB.TABLE_CODE is '从表编号'
;

comment on column FK_INFO_TAB.COL_CODE is '外键字段编号'
;

comment on column FK_INFO_TAB.ST_TM is '开始时间'
;

comment on column FK_INFO_TAB.END_TM is '结束时间'
;

comment on column FK_INFO_TAB.DATA_SRC is '数据来源'
;

create table FUNCTION_DEPENDENCY_TAB
(
	SYS_CODE VARCHAR(20) not null,
	TABLE_SCHEMA VARCHAR(20),
	TABLE_CODE VARCHAR(128) not null,
	LEFT_COLUMNS VARCHAR(512) not null,
	RIGHT_COLUMNS VARCHAR(64) not null,
	PROC_DT DATE,
	FD_LEVEL DECIMAL(5)
)
;

create unique index TMP_FUNCTION_DEPENDENCY_TABLE_CODE_INDEX
	on FUNCTION_DEPENDENCY_TAB (TABLE_CODE)
;

create table JOINT_FK_TAB
(
	FK_SYS_CODE VARCHAR(20) not null,
	FK_TABLE_CODE VARCHAR(128) not null,
	FK_COL_CODE VARCHAR(64) not null,
	GROUP_CODE VARCHAR(40) not null,
	SYS_CODE VARCHAR(20),
	TABLE_CODE VARCHAR(128),
	COL_CODE VARCHAR(64),
	ST_TM TIMESTAMP(6),
	END_TM TIMESTAMP(6)
)
;

create table JOINT_PK_TAB
(
	SYS_CODE VARCHAR(20) not null,
	TABLE_SCHEMA VARCHAR(20),
	TABLE_CODE VARCHAR(128) not null,
	GROUP_CODE VARCHAR(40) not null,
	COL_CODE VARCHAR(64) not null,
	ST_TM TIMESTAMP(6),
	END_TM TIMESTAMP(6)
)
;

create table MMM_FIELD_INFO_TAB
(
	SYS_CODE VARCHAR(20) not null,
	TABLE_SCHEMA VARCHAR(20) not null,
	TABLE_CODE VARCHAR(128) not null,
	COL_NUM INTEGER not null,
	COL_CODE VARCHAR(64) not null,
	COL_NAME VARCHAR(200),
	COL_COMMENT VARCHAR(512),
	COL_TYPE VARCHAR(32) not null,
	COL_LENGTH INTEGER not null,
	COL_NULLABLE CHAR(1) not null,
	COL_PK CHAR(1) not null,
	IS_STD CHAR(1) not null,
	CDVAL_NO CHAR(32),
	COL_CHECK CHAR(64),
	COLTRA CHAR(1),
	COLFORMAT VARCHAR(300),
	TRATYPE VARCHAR(30),
	ST_TM TIMESTAMP(6) not null,
	END_TM TIMESTAMP(6) not null,
	DATA_SRC VARCHAR(4),
	COL_AUTOINCRE CHAR(1),
	COL_DEFULT CHAR(1),
	COL_TYPE_JUDGE_RATE DECIMAL(19,5)
)
;

create table MMM_NODE_INFO
(
	NODE_CODE VARCHAR(50) not null,
	NODE_NAME VARCHAR(1024),
	NODE_TYPE VARCHAR(50),
	PARENT_NODE_CODE VARCHAR(50),
	SYS_CODE VARCHAR(5),
	DATA_SRC VARCHAR(4)
)
;

comment on table MMM_NODE_INFO is '节点信息表'
;

comment on column MMM_NODE_INFO.NODE_CODE is '节点编号'
;

comment on column MMM_NODE_INFO.NODE_NAME is '节点名称'
;

comment on column MMM_NODE_INFO.NODE_TYPE is '节点类型'
;

comment on column MMM_NODE_INFO.PARENT_NODE_CODE is '父节点编号'
;

comment on column MMM_NODE_INFO.SYS_CODE is '系统编号'
;

comment on column MMM_NODE_INFO.DATA_SRC is '数据来源'
;

create table MMM_TAB_INFO_TAB
(
	SYS_CODE VARCHAR(20) not null,
	TABLE_SCHEMA VARCHAR(20) not null,
	TABLE_CODE VARCHAR(128) not null,
	TABLE_NAME VARCHAR(200),
	TABLE_COMMENT VARCHAR(512),
	TABLE_OWNER VARCHAR(64) not null,
	ST_TM TIMESTAMP(6) not null,
	END_TM TIMESTAMP(6) not null,
	NODE_CODE VARCHAR(50),
	DATA_SRC VARCHAR(4),
	COL_NUM DECIMAL(9) not null
)
;

create table TMP_ALIAS_MAPPING
(
	ALIAS_TABLE_CODE VARCHAR(10),
	ORIGIN_TABLE_CODE VARCHAR(128),
	SYS_CODE VARCHAR(10),
	ID INTEGER
)
;

comment on table TMP_ALIAS_MAPPING is '原始表名映射表'
;

comment on column TMP_ALIAS_MAPPING.ALIAS_TABLE_CODE is '原始表映射名'
;

comment on column TMP_ALIAS_MAPPING.ORIGIN_TABLE_CODE is '原始表编号'
;

comment on column TMP_ALIAS_MAPPING.SYS_CODE is '系统编号'
;

comment on column TMP_ALIAS_MAPPING.ID is '别名序号'
;

CREATE TABLE CODE_INFO_TAB
(
  SYS_CODE        VARCHAR(20),
  TABLE_SCHEMA    VARCHAR(20),
  TABLE_CODE      VARCHAR(128),
  COLUMN_CODE     VARCHAR(64),
  CDVAL_TYPE      CHAR(1),
  CODE_CATE_NAME  VARCHAR(64),
  CODE_VALUE      VARCHAR(128),
  ST_TM           TIMESTAMP(6),
  CODE_CONTENT    VARCHAR(256)
);

CREATE VIEW FIELD_INFO_FEATURE_VIEW as (
SELECT
t1.SYS_CODE,
t1.TABLE_SCHEMA,
t1.TABLE_CODE,
t1.COL_CODE,
t2.COL_RECORDS,
t2.COL_DISTINCT,
t2.MAX_LEN,
t2.MIN_LEN,
t2.AVG_LEN,
t2.SKEW_LEN,
t2.KURT_LEN,
t2.MEDIAN_LEN,
t2.VAR_LEN,
t2.HAS_CHINESE,
t2.TECH_CATE,
t1.COL_NUM,
t1.COL_NAME,
t1.COL_COMMENT,
CASE WHEN COL_TYPE_JUDGE_RATE=1.0 THEN t1.COL_TYPE ELSE CASE WHEN t2.MAX_LEN=t2.MIN_LEN THEN 'CHARACTER' ELSE 'VARCHAR' END END AS COL_TYPE,
t1.COL_LENGTH,
t1.COL_NULLABLE,
t1.COL_PK,
t1.IS_STD,
t1.CDVAL_NO,
t1.COL_CHECK,
t1.COLTRA,
t1.COLFORMAT,
t1.TRATYPE,
t1.ST_TM,
t1.END_TM,
t1.DATA_SRC,
t1.COL_AUTOINCRE,
t1.COL_DEFULT,
t1.COL_TYPE_JUDGE_RATE
FROM MMM_FIELD_INFO_TAB t1, FEATURE_TAB t2
WHERE t1.SYS_CODE = t2.SYS_CODE AND t1.TABLE_SCHEMA = t2.TABLE_SCHEMA AND t1.TABLE_CODE = t2.TABLE_CODE AND t1.COL_CODE = t2.COL_CODE
);