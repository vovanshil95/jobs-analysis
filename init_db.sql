create table vacancy (
    id serial primary key,
    premium boolean,
    name_ text,
    department text,
    has_test text,
    response_letter_required boolean,
    area text,
    salary_from integer,
    salary_to integer,
    currency varchar(3),
    rur_salary_from integer,
    rur_salary_to integer,
    rur_salary_avg integer,
    type_ text,
    city text,
    street text,
    building text,
    lat float,
    lng float,
    full_address text,
    published_at timestamp,
    employer text,
    schedule text,
    accept_temporary boolean,
    professional_role text,
    experience text,
    employment text,
    description_ text,
    responded boolean,
    archived boolean,
    grade text,
    dt timestamp
);

create table key_skill (
    id serial primary key,
    name_ text
);

create table skill_vacancy (
    skill_id serial,
    vacancy_id serial
);