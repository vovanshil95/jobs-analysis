create table vacancy (
    id bigint primary key,
    premium boolean,
    name_ text,
    department text,
    has_test boolean,
    response_letter_required boolean,
    area text,
    salary_from bigint,
    salary_to bigint,
    currency varchar(3),
    rur_salary_from bigint,
    rur_salary_to bigint,
    rur_salary_avg bigint,
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
    id bigint primary key,
    name_ text
);

create table skill_vacancy (
    skill_id bigint,
    vacancy_id bigint
);