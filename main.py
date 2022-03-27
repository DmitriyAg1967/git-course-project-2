
import sqlalchemy
import vk_api
from random import randrange
from vk_api.longpoll import VkLongPoll, VkEventType
from datetime import date
import requests

with open('token_vk.txt', 'r') as file_object:
    token = file_object.read().strip()
with open('group_vk_id.txt', 'r') as file_object:
    group_id = file_object.read().strip()
with open('token_app_vk.txt', 'r') as file_object:
    app_token = file_object.read().strip()

engine = sqlalchemy.create_engine('postgresql+psycopg2://dmag:d123-v456-a789@localhost:5432/VK_DB')
connection = engine.connect()
connection.execute("""create table if not exists user_data (id serial primary key,id_seeker integer not null, id_vk integer not null, user_name text not null, user_age integer not null, gender integer not null)""")

vk = vk_api.VkApi(token=token)
longpoll = VkLongPoll(vk)

def write_msg(user_id, message):
    vk.method('messages.send', {'user_id': user_id, 'message': message,  'random_id': randrange(10 ** 7),})

birDate = {}

def parse_date_string(s):
    parts = s.split('.')
    birDate ={'day':int(parts[0]), 'month':int(parts[1]), 'year':int(parts[2])}
    return birDate

def calculateAge(birthDate):
    days_in_year = 365.2425
    age = int((date.today() - birthDate).days / days_in_year)
    return age

def user_info(user_id):
    global users_id
    global age
    global sex
    global relation
    user_get = vk.method('users.get', {'user_ids': user_id, 'fields': 'city, relation, sex, bdate'})
    users_id = user_get[0]['id']
    user_name = str(user_get[0]['first_name']) + ' ' + str(user_get[0]['last_name'])
    if 'city' in user_get[0]:
        city = user_get[0]['city']['title']
    else:
        write_msg(user_id, "Пожалуйста укажите город в котором Вы живете.")
        for event in longpoll.listen():
            if event.type == VkEventType.MESSAGE_NEW:
                if event.to_me:
                    request = event.text
                    city = request
                    break
    relation = user_get[0]['relation']
    sex = user_get[0]['sex']
    bdate = user_get[0]['bdate']
    x = parse_date_string(bdate)
    age = calculateAge(date(x['year'], x['month'], x['day']))
    if relation == 0:
        write_msg(user_id, "Пожалуйста укажите свое семейное положение. Введите: 1 — не женат/не замужем; 2 — есть друг/есть подруга; 3 — помолвлен/помолвлена; 4 — женат/замужем; 5 — всё сложно; 6 — в активном поиске; 7 — влюблён/влюблена; 8 — в гражданском браке;")
        for event in longpoll.listen():
            if event.type == VkEventType.MESSAGE_NEW:
                if event.to_me:
                    request = event.text
                    if request == '1':
                        relation = 1
                        write_msg(user_id, "Отлично, теперь есть всё для поиска пары.")
                        break
                    elif request == '2':
                        relation = 2
                        write_msg(user_id, "Отлично, теперь есть всё для поиска пары.")
                        break
                    elif request == '3':
                        relation = 3
                        write_msg(user_id, "Отлично, теперь есть всё для поиска пары.")
                        break
                    elif request == '4':
                        relation = 4
                        write_msg(user_id, "Отлично, теперь есть всё для поиска пары.")
                        break
                    elif request == '5':
                        relation = 5
                        write_msg(user_id, "Отлично, теперь есть всё для поиска пары.")
                        break
                    elif request == '6':
                        relation = 6
                        write_msg(user_id, "Отлично, теперь есть всё для поиска пары.")
                        break
                    elif request == '7':
                        relation = 7
                        write_msg(user_id, "Отлично, теперь есть всё для поиска пары.")
                        break
                    elif request == '8':
                        relation = 8
                        write_msg(user_id, "Отлично, теперь есть всё для поиска пары.")
                        break
                    else:
                        write_msg(user_id, "Вы ввели неправильный цифровой индекс.")
    if sex == 0:
        write_msg(user_id, "Пожалуйста укажите свой пол. Введите: 1 — если вы женщина; 2 — если вы мужчина;")
        for event in longpoll.listen():
            if event.type == VkEventType.MESSAGE_NEW:
                if event.to_me:
                    request = event.text
                    if request == '1':
                        sex = 1
                        break
                    elif request == '2':
                        sex = 2
                        break
                    else:
                        write_msg(user_id, "Вы ввели неправильный цифровой индекс.")
    z = [users_id, user_name, city, relation, sex, age]
    group_user_info(group_id)
    return z

def group_user_info(group_id):
    data_from_table = connection.execute("""SELECT id_seeker, id_vk FROM user_data;""").fetchmany(10)
    users_info = requests.get('https://api.vk.com/method/groups.getMembers', params={
            'access_token':token,
            'v':5.131,
            'group_id': group_id,
            'sort':'id_asc',
            'offset':0,
            'count' :100,
            'fields':'bdate, sex, relation'
        }).json()['response']['items']
    for users_date in users_info:
        id_users = users_date['id']
        name_users = str(users_date['first_name'] + " " + users_date['last_name'])
        sex_users = users_date['sex']
        relation_users = users_date['relation']
        y = parse_date_string(users_date['bdate'])
        age_users = calculateAge(date(y['year'], y['month'], y['day']))
        URL = 'https://api.vk.com/method/photos.get'
        params = {
            'owner_id': id_users,
            'album_id': 'wall',
            'extended': '1',
            'access_token': app_token,
            'v': '5.131'
        }
        res = requests.get(URL, params=params)
        if relation == 4:
            write_msg(users_id, "Ой Вы кажется женаты или замужем. Короче увы...")
            break
        if abs(int(age) - int(age_users)) < 5 and int(sex) != int(sex_users) and relation != 4 and relation_users != 4:
            pair = (users_id, id_users)
            if pair in data_from_table:
                write_msg(users_id, "Этого кандидата я уже предлагал...")
            elif pair not in data_from_table:
                write_msg(users_id, f"Это тот кто Вам подходит: https://vk.com/id{id_users}.")
                str_1 = "INSERT INTO user_data (id_seeker, id_vk, user_name, user_age, gender) VALUES (%s, %s,'%s',%s,%s)" % (users_id, id_users, name_users, age_users, sex_users)
                connection.execute(str_1)
                i = 0
                likes = 0
                for res_1 in res.json()['response']['items']:
                    likes_init = res_1['likes']['count']
                    if likes_init > likes and likes_init != 0 and i < 3:
                        i += 1
                        likes = likes_init
                        for res_2 in res_1['sizes']:
                            if res_2['type'] == 'z':
                                url_photo = res_2['url']
                                write_msg(users_id, {url_photo})
                            else:
                                continue
                    elif likes_init == likes and likes_init != 0 and i < 3:
                        i += 1
                        likes = likes_init
                        for res_2 in res_1['sizes']:
                            if res_2['type'] == 'z':
                                url_photo = res_2['url']
                                write_msg(users_id, {url_photo})
                    else:
                        continue
            else:
                continue
        else:
            continue
    return users_info

for event in longpoll.listen():
    if event.type == VkEventType.MESSAGE_NEW:
        if event.to_me:
            request = event.text
            if request == "привет бот" or request == "Привет бот":
                user_get = vk.method('users.get', {'user_ids': event.user_id, 'fields': 'city, relation, sex, bdate'})
                first_name = str(user_get[0]['first_name'])
                write_msg(event.user_id, f"Привет, {first_name}, я бот, хочешь я найду тебе пару.")
                for event in longpoll.listen():
                    if event.type == VkEventType.MESSAGE_NEW:
                        if event.to_me:
                            request = event.text
                            if request == "да" or request == "Да":
                                user_info(event.user_id)
                            else:
                                write_msg(event.user_id, "Не понял, жду 'привет бот'...")
                                break
            else:
                write_msg(event.user_id, "Не понял,напишите 'привет бот'...")
                continue