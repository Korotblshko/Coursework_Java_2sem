# Coursework: Thread Pool and Mini RxJava

Курсовая работа по дисциплине «Многопоточное и асинхронное программирование на Java».

Проект содержит две части:

1. собственная реализация пула потоков;
2. учебная реализация библиотеки, похожей на RxJava.

Проект написан на Java 17 и собирается через Maven.

## Структура

- `ru.coursework.threadpool` — реализация кастомного Thread Pool.
- `ru.coursework.rxjava` — реализация Mini RxJava.
- `src/test/java/ru/coursework/rxjava` — JUnit-тесты для Mini RxJava.

## Задание 1. Custom Thread Pool

В первой части реализован собственный пул потоков без использования `ThreadPoolExecutor`.

Поддерживается:

- `corePoolSize`;
- `maxPoolSize`;
- `keepAliveTime`;
- ограниченный размер очереди;
- `minSpareThreads`;
- `execute(Runnable)`;
- `submit(Callable<T>)`;
- `Future<T>`;
- `shutdown()`;
- `shutdownNow()`;
- обработка отказов при перегрузке;
- логирование работы пула.

Основные классы:

- `CustomExecutor` — интерфейс управления пулом;
- `CustomThreadPool` — основная реализация;
- `Worker` — рабочий поток со своей очередью;
- `CustomThreadFactory` — фабрика потоков с уникальными именами;
- `AbortRejectedTaskHandler` — политика отказа;
- `Main` — демонстрация работы.

Задачи распределяются между worker-ами по принципу Round Robin. Если очередь выбранного worker-а заполнена, пул пытается найти наименее загруженную очередь. Если все очереди заполнены и достигнут `maxPoolSize`, задача отклоняется через `RejectedExecutionException`.

Для демонстрации использовались параметры:

- `corePoolSize = 2`
- `maxPoolSize = 4`
- `queueSize = 5`
- `keepAliveTime = 5 секунд`
- `minSpareThreads = 1`

Стандартный `ThreadPoolExecutor` является более надежным и оптимизированным решением для промышленного использования. Кастомный пул уступает ему по зрелости, но демонстрирует основные принципы: очереди задач, создание потоков, балансировку, обработку перегрузки и корректное завершение.

## Задание 2. Mini RxJava

Во второй части реализована учебная версия библиотеки, похожей на RxJava.

Реализовано:

- `Observable`;
- `Observer`;
- `Emitter`;
- `Disposable`;
- `map`;
- `filter`;
- `flatMap`;
- `onError`;
- `onComplete`;
- `subscribeOn`;
- `observeOn`;
- `Scheduler`.

Schedulers:

- `IOThreadScheduler` — использует `CachedThreadPool`;
- `ComputationScheduler` — использует `FixedThreadPool`;
- `SingleThreadScheduler` — использует один поток.

`Observable` создает поток данных. `Observer` подписывается на него и получает события через `onNext`, `onError` и `onComplete`.

Оператор `map` преобразует элементы, `filter` отбирает нужные элементы, `flatMap` преобразует элемент в новый `Observable`.

Метод `subscribeOn` задает поток, где выполняется источник данных. Метод `observeOn` задает поток, где выполняется обработчик подписчика.

Для отмены подписки используется `Disposable`. Ошибки из источника или операторов передаются в `onError`.

## Тестирование

Для Mini RxJava написаны JUnit-тесты.

Проверяется:

- базовая подписка;
- `map`;
- `filter`;
- `flatMap`;
- обработка ошибок;
- отмена подписки;
- `subscribeOn`;
- `observeOn`;
- `ComputationScheduler`.

Запуск тестов:

`mvn clean test`

Успешный результат:

`Tests run: 10, Failures: 0, Errors: 0, Skipped: 0`

## Запуск

Запуск тестов:

`mvn clean test`

Запуск демонстрации Thread Pool:

`mvn exec:java`

Или класс:

`ru.coursework.threadpool.Main`

Запуск демонстрации Mini RxJava:

`ru.coursework.rxjava.RxDemo`

## Результат

Thread Pool демонстрирует создание потоков, распределение задач, выполнение `Runnable` и `Callable`, отклонение задач при перегрузке и корректное завершение.

Mini RxJava демонстрирует работу `map`, `filter`, `flatMap`, `subscribeOn`, `observeOn` и передачу ошибок в `onError`.
