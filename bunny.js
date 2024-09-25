const axios = require('axios');
const fs = require('fs');
const path = require('path');
const { Worker, isMainThread, parentPort, workerData } = require('worker_threads');
const ProgressBar = require('progress');

// Funkcja do sprawdzenia istniejącej kolekcji
async function getCollection(modelName, apiKey, libraryId) {
    const collectionsUrl = `https://video.bunnycdn.com/library/${libraryId}/collections`;
    const headers = { 'AccessKey': apiKey, 'Content-Type': 'application/json' };

    try {
        console.log(`Sprawdzanie kolekcji dla modelki: ${modelName}`);
        const collectionsResponse = await axios.get(collectionsUrl, { headers });
        console.log(`Odpowiedź API (kolekcje):`, collectionsResponse.data);

        const collections = collectionsResponse.data.items;
        return collections.find(collection => collection.name === modelName);

    } catch (error) {
        console.error(`Błąd podczas pobierania kolekcji: ${error.response ? error.response.data.Message : error.message}`);
        throw error;
    }
}

// Funkcja do tworzenia nowej kolekcji
async function createCollection(modelName, apiKey, libraryId) {
    const collectionsUrl = `https://video.bunnycdn.com/library/${libraryId}/collections`;
    const headers = { 'AccessKey': apiKey, 'Content-Type': 'application/json' };

    try {
        console.log(`Tworzenie nowej kolekcji dla modelki: ${modelName}`);
        const newCollectionResponse = await axios.post(collectionsUrl, { name: modelName }, { headers });
        console.log(`Odpowiedź API (nowa kolekcja):`, newCollectionResponse.data);

        if (!newCollectionResponse.data || !newCollectionResponse.data.guid) {
            console.error('Brak GUID nowej kolekcji w odpowiedzi API.');
            console.error('Odpowiedź:', newCollectionResponse.data);
            throw new Error('GUID kolekcji jest undefined.');
        }

        return newCollectionResponse.data.guid;

    } catch (error) {
        console.error(`Błąd podczas tworzenia kolekcji: ${error.response ? error.response.data : error.message}`);
        if (error.response) {
            console.error('Szczegóły odpowiedzi:', error.response.status, error.response.data);
        }
        throw error;
    }
}

// Funkcja do sprawdzenia lub utworzenia kolekcji
async function getOrCreateCollection(modelName, apiKey, libraryId) {
    const existingCollection = await getCollection(modelName, apiKey, libraryId);
    if (existingCollection) {
        console.log(`Znaleziono istniejącą kolekcję: ${existingCollection.guid || 'undefined'}`);
        return existingCollection.guid;
    }

    return await createCollection(modelName, apiKey, libraryId);
}

// Funkcja do tworzenia wideo z walidacją odpowiedzi API
async function createVideo(libraryId, collectionId, title, apiKey) {
    const url = `https://video.bunnycdn.com/library/${libraryId}/videos`;
    const headers = { 'AccessKey': apiKey, 'Content-Type': 'application/json' };
    const data = { title, collectionId };

    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            console.log(`Tworzenie wideo: ${title} (próba ${attempt})`);
            const response = await axios.post(url, data, { headers });
            console.log(`Odpowiedź API (tworzenie wideo):`, response.data);

            if (!response.data.guid) {
                throw new Error('Brak GUID wideo w odpowiedzi API.');
            }

            return response.data.guid;

        } catch (error) {
            console.error(`Błąd podczas tworzenia wideo: ${error.response ? error.response.data.Message : error.message}`);
            if (attempt === 3 || error.response.status !== 500) {
                throw error; // Po 3 próbach lub innym błędzie niż 500, przerwij
            }
            console.log('Ponawiam próbę stworzenia wideo...');
        }
    }
}

// Funkcja do uploadu wideo strumieniowo z walidacją odpowiedzi API
async function uploadVideo(libraryId, videoId, videoPath, apiKey) {
    const url = `https://video.bunnycdn.com/library/${libraryId}/videos/${videoId}`;
    const headers = { 'AccessKey': apiKey, 'Content-Type': 'application/octet-stream' };

    for (let attempt = 1; attempt <= 3; attempt++) {
        try {
            console.log(`Przesyłanie wideo: ${path.basename(videoPath)} (próba ${attempt})`);
            const videoStream = fs.createReadStream(videoPath);

            const response = await axios({
                method: 'PUT',
                url: url,
                headers: headers,
                data: videoStream
            });

            console.log(`Wideo ${path.basename(videoPath)} zostało przesłane pomyślnie.`);
            return true;

        } catch (error) {
            console.error(`Błąd podczas przesyłania wideo: ${error.response ? error.response.data.Message : error.message}`);
            if (attempt === 3 || error.response.status !== 500) {
                throw error; // Po 3 próbach lub innym błędzie niż 500, przerwij
            }
            console.log('Ponawiam próbę przesłania wideo...');
        }
    }
}

// Funkcja do przetwarzania plików wideo z wątkami
async function processVideoInThread({ videoFile, folderPath, apiKey, libraryId }) {
    const videoPath = path.join(folderPath, videoFile);

    const [modelName, recordingDate] = videoFile.split('_');
    const title = `${modelName} ${recordingDate}`;

    try {
        const collectionId = await getOrCreateCollection(modelName, apiKey, libraryId);
        const videoId = await createVideo(libraryId, collectionId, title, apiKey);
        await uploadVideo(libraryId, videoId, videoPath, apiKey);

        fs.unlinkSync(videoPath);
        return { success: true, videoFile };
    } catch (error) {
        return { success: false, videoFile, error: error.message };
    }
}

// Funkcja główna w wątku
if (!isMainThread) {
    processVideoInThread(workerData)
        .then(result => parentPort.postMessage(result))
        .catch(error => parentPort.postMessage({ success: false, error: error.message }));
}

// Funkcja do przetwarzania plików wideo z limitem wątków
async function processVideosWithThreadLimit(folderPath, apiKey, libraryId, maxThreads) {
    const videoFiles = fs.readdirSync(folderPath).filter(file => path.extname(file) === '.mp4');
    
    // Pasek postępu
    const bar = new ProgressBar('[:bar] :percent - Przetwarzanie plików wideo', {
        total: videoFiles.length,
        width: 30
    });

    let runningWorkers = 0;
    let currentIndex = 0;

    return new Promise((resolve, reject) => {
        function startNextWorker() {
            if (currentIndex >= videoFiles.length) {
                if (runningWorkers === 0) {
                    resolve();
                }
                return;
            }

            const videoFile = videoFiles[currentIndex];
            currentIndex++;
            runningWorkers++;

            const worker = new Worker(__filename, {
                workerData: { videoFile, folderPath, apiKey, libraryId }
            });

            worker.on('message', (result) => {
                runningWorkers--;
                bar.tick();
                if (result.success) {
                    console.log(`Plik ${result.videoFile} został przetworzony.`);
                } else {
                    console.error(`Błąd przetwarzania pliku ${result.videoFile}: ${result.error}`);
                }
                startNextWorker();
            });

            worker.on('error', (error) => {
                runningWorkers--;
                console.error(`Błąd w wątku dla pliku ${videoFile}: ${error.message}`);
                startNextWorker();
            });

            worker.on('exit', () => {
                runningWorkers--;
                startNextWorker();
            });
        }

        for (let i = 0; i < maxThreads && i < videoFiles.length; i++) {
            startNextWorker();
        }
    });
}

// Główna funkcja
async function uploadToBunnyStream() {
    const libraryId = '314887';  // Zamień na swoje ID biblioteki
    const apiKey = '271d80bb-3ebd-4eab-aedf951bf504-04f6-45f2';        // Twój klucz API Bunny Stream
    const folderPath = './converted';   // Ścieżka do folderu z plikami wideo
    const maxThreads = 2;            // Maksymalna liczba jednoczesnych wątków

    console.log('Rozpoczynam przetwarzanie plików wideo...');
    await processVideosWithThreadLimit(folderPath, apiKey, libraryId, maxThreads);
    console.log('Wszystkie pliki zostały przetworzone.');
}

if (isMainThread) {
    uploadToBunnyStream().catch(console.error);
}
