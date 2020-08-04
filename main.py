
import psycopg2 as pg
from pprint import pprint


PARAMS = {'database': 'netology_db',
          'user': 'alex_p',
          'password': 'ujuf,tkblpt',
          'host': 'localhost',
          'port': 5432}


def drop():
    with pg.connect(**PARAMS) as conn:
        cur = conn.cursor()
        cur.execute('''
        drop table if exists Student_Course;
        drop table if exists Student;
        drop table if exists Course;
        ''')


def get_course(course_name):
    with pg.connect(**PARAMS) as conn:
        cur = conn.cursor()
        cur.execute('insert into Course (course_name) values(%s);',
                    (course_name,)
                    )


def create_db():  # создает таблицы
    with pg.connect(**PARAMS) as conn:
        cur = conn.cursor()
        cur.execute('''
                    create table if not exists Student (
                                            student_id serial primary key,
                                            name varchar(100),
                                            gpa numeric(10,2) null,
                                            birth timestamp with time zone null);
                    ''')
        cur.execute('''
                    create table if not exists Course (
                                            course_id serial primary key,
                                            course_name varchar(100));

                    ''')
        cur.execute('''
                    create table if not exists Student_Course (
                                            id serial primary key,
                                            student_id integer references student(student_id),
                                            course_id integer references course(course_id));
                    ''')


def add_student(student):  # просто создает студента
    with pg.connect(**PARAMS) as conn:
        cur = conn.cursor()
        cur.execute('insert into Student (name, gpa, birth) values (%s, %s, %s);',
                    (student.get('name'), student.get('gpa'), student.get('birth'))
                    )


def get_student(student_id):
    with pg.connect(**PARAMS) as conn:
        cur = conn.cursor()
        cur.execute('select * from Student where student_id = (%s);',
                    (student_id,)
                    )
    print(cur.fetchone())


def get_students(course_id):  # возвращает студентов определенного курса
    with pg.connect(**PARAMS) as conn:
        cur = conn.cursor()
        cur.execute('''
                    select c.course_name, s.student_id, s.name, s.gpa, s.birth from Student_Course sc
                    join Student s on s.student_id = sc.student_id
                    join Course c on c.course_id = sc.course_id
                    where c.course_id = (%s);
                    ''',
                    (course_id,))
    pprint(cur.fetchall())


def add_students(course_id, students):  # создает студентов и записывает их на курс
    with pg.connect(**PARAMS) as conn:
        cur = conn.cursor()
        if course_id:
            for data in students:
                cur.execute('insert into Student (name, gpa, birth) values (%s, %s, %s) returning student_id;',
                            (data.get('name'), data.get('gpa'), data.get('birth'))
                            )
                students_id = cur.fetchone()
                cur.execute('insert into Student_Course (student_id, course_id) values (%s, %s);',
                            (students_id, course_id)
                            )


if __name__ == '__main__':

    students = [{'name': 'Ivam Nehvorat', 'gpa': 4.73, 'birth': '2002-05-16'},
                {'name': 'Sub Zero', 'gpa': 3.15, 'birth': '2001-11-10'},
                {'name': 'Vladimir Pupin', 'gpa': 4.99, 'birth': '2001-08-19'}
                ]

    # drop()
    # create_db()
    # get_course('python')
    # get_all()
    # add_students(1, students)
    get_students(1)
    get_student(2)
