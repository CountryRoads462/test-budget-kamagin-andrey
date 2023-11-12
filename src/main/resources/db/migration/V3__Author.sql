create table author
(
    id         serial    primary key,
    full_name  text      not null,
    created_at timestamp not null default now()
);