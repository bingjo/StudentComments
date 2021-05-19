import requests
import json
import sqlite3
from collections import Counter
from datetime import datetime



def f(lst, n):
    return [lst[i:i + n] for i in range(0, len(lst), n)]


def get_groups():
    conn = sqlite3.connect('vkGroups.db')
    cur = conn.cursor()

    numFile = '4'
    users = open('Result/' + numFile + '.txt', encoding="utf8").read().split('\n')  # Получение id всех пользователей в группе
    users = f(users, 200)  # Разбиение на группы по 200 человек

    for i in range(len(users)):
        
        print(i)
        
        t = ','.join(map(str, users[i]))

        usersGet = requests.get('https://api.vk.com/method/users.get?user_ids=' + t + '&access_token=1234567890v=5.126')  # Запрос информации по 200-стам людям
        if usersGet.status_code == 200:
            
            todos = json.loads(usersGet.text)

            if 1 == 1:
                
                for j in range(len(todos['response'])):  # Цикл по 200-стам пользователям

                    UserId = str(todos['response'][j]['id'])  # Получение id пользователя
                    
                    UserIdDB = str(users[i][j])  # Сохрание публичной ссылки пользователя
                    UserNumIdDB = str(UserId)  #  Сохранение id пользователя
                    try:
                        IsClosedUserDB = todos['response'][j]['is_closed']  # Закрытый или открытый профиль
                    except:
                        IsClosedUserDB = False

                    UserWriteDB = (UserIdDB, UserNumIdDB, IsClosedUserDB, numFile)  # Запись в БД пользователя
                    cur.execute("INSERT INTO Users (UserId, UserNumId, IsClosed, G) VALUES(?, ?, ?, ?);", UserWriteDB)
                    CommitId = cur.lastrowid
                    conn.commit()

                    
                    UserdGroups = requests.get('https://api.vk.com/method/groups.get?user_id=' + UserId + '&access_token=1234567890&count=999&extended=1&fields=activity,description&v=5.126')  # Получение всех групп пользователя
                    
                    if UserdGroups.status_code == 200:
                        todosGroups = json.loads(UserdGroups.text)
                        try:
                            errr = todosGroups['error']['error_code']
                            if errr == 5 or errr == 28:
                                exit(0)
                        except:
                            pass
                        try:
                            count = todosGroups['response']['count']  # Получение кол-ва групп пользователя
                        except:
                            count = 0
                        if count > 0:
                            for h in range(len(todosGroups['response']['items'])):
                                GroupIdDB = todosGroups['response']['items'][h]['id']  # Получение id группы
                                GroupNameDB = todosGroups['response']['items'][h]['name']  # Получение имени группы
                                try:
                                    GroupThemeDB = todosGroups['response']['items'][h]['activity']  # Получение темы группы
                                except:
                                    GroupThemeDB = 'None'
                                IsClosedGroupDB = todosGroups['response']['items'][h]['is_closed']  # Закрытая или открытая группа
                                if IsClosedGroupDB == 0:
                                    IsClosedGroupDB = False
                                else:
                                    IsClosedGroupDB = True
                                
                                GroupWriteId = (GroupIdDB, GroupNameDB, GroupThemeDB, IsClosedGroupDB, CommitId)
                                cur.execute("INSERT INTO Groups (GroupId, GroupName, GroupTheme, IsClosed, UserId) VALUES(?, ?, ?, ?, ?);", GroupWriteId)
                                conn.commit()
                            
                        
                    else:
                        file2 = open('anew/users.txt', 'a')
                        file2.write(t + '\n')
                        file2.close()
                    

        else:
            file1 = open('anew/users.txt', 'a')
            file1.write(t + '\n')
            file1.close()


def get_post():
    # Получение всех постов в группе до 1 января 2020 года
    groupIdList = [-165020798]

    deadline = datetime(2020, 1, 1)

    conn = sqlite3.connect('vkGroups.db')
    cur = conn.cursor()

    for hh in range(len(groupIdList)):

        i = 0
        rrr = 0
    
        while True:

            url = 'https://api.vk.com/method/wall.get?owner_id=%s&offset=%s&access_token=1234567890&v=5.126&count=100'%(groupIdList[hh], i)
            wallGet = requests.get(url)

            todosWall = json.loads(wallGet.text)
            todosWall = todosWall['response']['items']

            print(i)

            for j in range(len(todosWall)):
                postId = todosWall[j]['id']

                groupId = todosWall[j]['owner_id']

                postText = todosWall[j]['text']

                try:
                    postPhoto = str(todosWall[j]['attachments'][0]['photo']['sizes'][-1]['url'])
                except:
                    postPhoto = ''

                try:
                    postComments = todosWall[j]['comments']['count']
                except:
                    postComments = 0

                try:
                    postLikes = todosWall[j]['likes']['count']
                except:
                    postLikes = 0

                try:
                    postReposts = todosWall[j]['reposts']['count']
                except:
                    postReposts = 0

                try:
                    postViews = todosWall[j]['views']['count']
                except:
                    postViews = 0
                
                
                timestamp = todosWall[j]['date']
                postDate = datetime.fromtimestamp(timestamp)

                if postDate < deadline:
                    rrr = 1
                    break
                else:
                    if len(postText) > 1 or len(postPhoto) > 1:
                        PostWriteDB = (groupId, postId, postText, postPhoto, postComments, postLikes, postReposts, postViews, postDate)
                        cur.execute("INSERT INTO Posts (GroupId, PostId, PostText, PostPhoto, PostComments, PostLikes, PostReposts, PostViews, PostTime) VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?);", PostWriteDB)
                        conn.commit()

            if rrr == 1:
                break
            i += 100


def get_comments_in_posts():
    # Получение всех комментариев студентов к посту
    conn = sqlite3.connect('vkGroups.db')
    cur = conn.cursor()

    f1 = open('Result/id/1.txt', encoding="utf8").read().split('\n')
    f2 = open('Result/id/1.txt', encoding="utf8").read().split('\n')
    idList = list(set(f1 + f2))

    p1 = open('Posts/1.txt', encoding="utf8").read().split('\n')
    p2 = open('Posts/2.txt', encoding="utf8").read().split('\n')
    PostsList = list(set(p1+p2))
    PostsList.sort()
    print(len(PostsList))

    GroupId = -143381428
    
    for i in range(len(PostsList)):
        try:
            h = 0
            print('Post',i)
            while True:
                
                getComments = requests.get('https://api.vk.com/method/wall.getComments?owner_id=' + str(GroupId) + '&post_id=' + PostsList[i] + '&access_token=1234567890&v=5.126&count=100&offset=' + str(h))
                todosComments = json.loads(getComments.text)
                todosComments = todosComments['response']['items']

                if len(todosComments) == 0:
                    break
                else:
                    for j in range(len(todosComments)):
                        try:
                            CommentId = str(todosComments[j]['id'])
                            UserId = str(todosComments[j]['from_id'])
                            PostId = str(PostsList[i])
                            CommenText = todosComments[j]['text']
                            if UserId in idList:
                                if len(CommenText) > 2:
                                    CommentWriteDB = (str(GroupId), str(PostId), str(UserId), '', str(CommenText))
                                    cur.execute("INSERT INTO CommentsInPosts (GroupId, PostId, UserId, CommentText, UserText) VALUES(?, ?, ?, ?, ?);", CommentWriteDB)
                                    conn.commit()
                                    print('C+')
                            getAnswer = requests.get('https://api.vk.com/method/wall.getComments?owner_id=' + str(GroupId) + '&post_id=' + str(PostsList[i]) + '&access_token=1234567890&v=5.126&count=100&comment_id=' + str(CommentId))
                            todosAnswer = json.loads(getAnswer.text)
                            todosAnswer = todosAnswer['response']['items']
                            for b in range(len(todosAnswer)):
                                AnsUserId = str(todosAnswer[b]['from_id'])
                                AnswerText = todosAnswer[b]['text']
                                if AnsUserId in idList:
                                    if len(AnswerText) > 2:
                                        CommentWriteDB = (str(GroupId), str(PostId), str(AnsUserId), str(CommenText), str(AnswerText))
                                        cur.execute("INSERT INTO CommentsInPosts (GroupId, PostId, UserId, CommentText, UserText) VALUES(?, ?, ?, ?, ?);", CommentWriteDB)
                                        conn.commit()
                                        print('A+')
                        except:
                            pass
                    h += 100
        except:
            print('ERR')

                
get_comments_in_posts()
