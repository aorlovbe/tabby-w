function getPrize(country) {
  // Определяем пул призов в зависимости от страны
  const rewardsPool = [
    "r-1",
    "r-2",
    "r-3",
    "r-4",
    "r-5",
    country === "ARE" ? "r-6" : "r-7",
    "r-8",
    "r-9",
    "r-10",
    country === "ARE" ? "r-11" : "r-12",
  ];

  // Генерируем случайное число от 0 до 99
  const roll = Math.floor(Math.random() * 100);

  let reward;
  if (roll >= 98) {
    return rewardsPool[rewardsPool.length - 1];
  } else {
    const partSize = 97 / (rewardsPool.length - 1);
    const index = Math.min(Math.floor(roll / partSize), rewardsPool.length - 2);

    return rewardsPool[index];
  }
}

// Пример использования
console.log(getPrize("ARE")); // для ОАЭ
console.log(getPrize("OTHER")); // для других стран

// Функция для проверки статистики
function testStatistics(country, iterations = 1000) {
  const results = {};

  for (let i = 0; i < iterations; i++) {
    const prize = getPrize(country);
    results[prize] = (results[prize] || 0) + 1;
  }

  const total = iterations;
  console.log(
    `Статистика выпадения призов (${iterations} испытаний для ${country}):`
  );
  for (const prize in results) {
    const percentage = ((results[prize] / total) * 100).toFixed(2);
    console.log(`${prize}: ${results[prize]} раз (${percentage}%)`);
  }
}

// Тестируем статистику
testStatistics("ARE");
testStatistics("OTHER");
