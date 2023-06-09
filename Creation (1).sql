CREATE TABLE Date_Dim
(
  Date_key date CONSTRAINT D_pk_cons PRIMARY KEY,
  Year number(5),
  Quarter number(1),
  Month_Name VARCHAR2(15),
  Month number(2),
  Day_Name VARCHAR2(20),
  Day_Number NUMBER(3)
  );
  
  
  
  Create Table Industry_Dim
(
    Industry_key number  CONSTRAINT industry_pk_cons PRIMARY KEY,
    Industry_Type varchar2(100)
 
 );
 
 
 
 Create Table Company_Dim
(
    Company_key number  CONSTRAINT company_pk_cons PRIMARY KEY,
    Company_name varchar2(200),
    location varchar2(50)
 );
 
 

 
Create Table Stock_Market_Fact
(
Company_ID  number  CONSTRAINT company_Id_Fk references company_dim(Company_key) ,
Industry_id Number constraint industry_id_fk  references Industry_Dim(Industry_key),
Date_key Date Constraint Reservation_Date_FK references date_dim(Date_Key),
close number(10,2),
open number(10,2),
Low number(10,2),
High number(10,2),
Volume number(10,2),
Change number(10,2)
 ); 
 
 
 
 
 
 
 
 
 