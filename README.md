# key-value-storage

Реализация key-value хранилища с методами шардирования и репликации с использованием учебного фреймворка [dslib](https://github.com/osukhoroslov/dslib) - методом шардирования консистентным хэшированием, как в [Chord](https://pdos.csail.mit.edu/papers/ton:chord/paper-ton.pdf), и методом репликации и разрешения конфликтов, как в [Amazon Dynamo](https://www.allthingsdistributed.com/files/amazon-dynamo-sosp2007.pdf).
