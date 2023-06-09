*Реализованы базовый функционал, разрешение конфликтов, sloppy-quorum и hinted handoff.*

_______

**Базовый функционал и разрешение конфликтов**


Информация о текущих запросах хранится на ноде в трех словарях pending {номер запроса: класс с информацией о запросе} (для каждого вида запросов: get, put, delete отдельно). Когда на ноду приходит запрос, она добавляет его в словарь по его порядковому номеру и отправляет запрос на реплики искомого ключа. В классе с информацией о запросе хранятся поля кворум, ключ, пара [значение, время], число полученных ответов и не ответившие ноды. В словаре self._data хранятся пары [значение, время].

1. GET

Если ноде приходит запрос GET с local, она пересылает этот запрос всем репликам, на которых должен храниться ключ.

Если ноде приходит запрос GET с другой ноды, она отправляет обратно пару [значение, время] из словаря self._data.

Нода, которой пришел ответ, сверяет полученные время и значение с теми, что хранятся в словаре pending для данного запроса. Из этих двух пар в информацию о запросе записывается та, где больше время или при их равенстве та, где значение больше лексикографически. Число полученных ответов увеличивается на один. Если оно стало равно кворуму, на local отправляется подтверждение со значением, записанным в информацию о запросе, и информация о запросе удаляется. Кроме того, всем репликам отправляется финальная пара, чтобы они могли обновить значения у себя.

2. PUT

Если ноде приходит запрос PUT с local, она вкладывает в этот запрос пару [значение, текущее время] и пересылает этот запрос всем репликам, на которых должен храниться ключ.

Если ноде приходит запрос PUT с другой ноды, она сравнивает полученную пару с той, что хранится в self._data. Выигравшая пара отправляется обратно.

Нода, которой пришел ответ, сверяет полученные время и значение с теми, что хранятся в словаре pending для данного запроса. Из этих двух пар в информацию о запросе записывается большая. Число полученных ответов увеличивается на один. Если оно стало равно кворуму, на local отправляется подтверждение со значением, записанным в информацию о запросе, и информация о запросе удаляется.

3. DELETE

Если ноде приходит запрос DELETE с local, она вкладывает в этот запрос пару [None, текущее время] и пересылает этот запрос всем репликам, на которых должен храниться ключ.

Если ноде приходит запрос DELETE с другой ноды, она сравнивает полученную пару с той, что хранится в self._data. Выигравшая пара записывается в self._data. Обратно отправляется пара, которая изначально была записана в self._data.

Нода, которой пришел ответ, сверяет полученные время и значение с теми, что хранятся в словаре pending для данного запроса. Из этих двух пар в информацию о запросе записывается большая. Число полученных ответов увеличивается на один. Если оно стало равно кворуму, на local отправляется подтверждение со значением, записанным в информацию о запросе, и информация о запросе удаляется.

_______

**Sloppy-quorum и hinted handoff**

Для хранения дополнительной информации на каждой ноде хранится словарь self._handed_data, где хранятся значения, которую ему временно передали, и словарь self._handed_values, где по индивидуальным иденификаторам хранится служебная информация о каждой переданной ему операции.

1. GET

Если ноде, которая отвечает за запрос GET, не ответили какие-то из реплик, она после срабатывания таймера спрашивает у следующих за ними нод, пока не получит quorum ответов всего.

2. PUT

Если ноде, которая отвечает за запрос PUT, не ответили какие-то из реплик, она после срабатывания таймера пересылает этот запрос на следующие за ними ноды в виде PUT_HANDOFF, чтобы те положили эту информацию на время к себе. Нода, которая получила PUT_HANDOFF, сверяет полученной значение с тем, которое ей уже передавали (если такое было), если полученное новее, обновляет. И отправляет обратно итоговое значение. После этого она запускает таймер, при срабатывании которого пытается передать информацию той реплике, которой она предназначалась. Если она получит ответ, она удалит у себя эту информацию об этой операции.

3. DELETE

Почти аналогично PUT, но кладется значение [None, time], и обратно отправляется предыдущее значение.

___________
