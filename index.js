const {DuolingoClient} = require('duolingo-client')
const fs = require('fs')
const mkdirp = require('mkdirp')
const {MongoClient} = require('mongodb')
const Multimap = require('multimap')
const {dirname} = require('path')
const sha1 = require('sha1')

const CACHE_DIR = 'out'

const COURSE_IDS = [
    'DUOLINGO_VI_EN',
    'DUOLINGO_EN_VI',
]

async function main() {
    const duolingo = await createDuolingoClient()
    const mongo = await createMongoClient()
    const loader = new Loader(duolingo, mongo)
    try {
        await loader.update(COURSE_IDS)
    }
    finally {
        await mongo.close()
    }
}

async function createDuolingoClient() {
    const data = fs.readFileSync('secrets/duo.json')
    const {user, password} = JSON.parse(data)
    const duolingo = new DuolingoClient()
    await duolingo.login(user, password)
    return duolingo
}

async function createMongoClient() {
    const data = fs.readFileSync('secrets/db.json')
    const {url, user, password} = JSON.parse(data)
    const mongo = new MongoClient(url, {
        useNewUrlParser: true,
        auth: {user, password},
    })
    await mongo.connect()
    return mongo
}

async function cache(path, populateFunction) {
    const file = `${CACHE_DIR}/${path}`
    const dir = dirname(file)
    await mkdirp(dir)

    let data
    try {
        // TODO: async
        data = JSON.parse(fs.readFileSync(file))
        console.log('cache read: ' + file)
    }
    catch (err) {
        console.log('cache miss: ' + file)
        data = await populateFunction()
        // TODO: async
        fs.writeFileSync(file, JSON.stringify(data, null, '  '))
        console.log('cache write: ' + file)
    }
    return data
}

class Loader {
    constructor(duolingo, mongo) {
        this.duolingo = duolingo
        this.mongo = mongo
    }

    async update(courseIds) {
        const allCourses = await this.getCourses()
        const courses = allCourses.filter(it => courseIds.includes(it.id))

        // assumes at least one course is learning English
        const languages = courses.map(it => it.learningLanguage)
        await this.writeLanguages(languages)

        for (const course of courses) {
            await this.updateCourse(course)
        }
    }

    async updateCourse(course) {
        const from = course.learningLanguage.id
        const to = course.fromLanguage.id

        console.log('setCurrentCourse:', course.id)
        await this.duolingo.setCurrentCourse(course.id)

        const skills = await this.getCourseSkills(course.id)
        skills.forEach((skill, order) => {
            skill.from = from
            skill.to = to
            skill.order = order
        })
        await this.writeSkills(skills)

        const wordsToSkills = new Multimap()
        for (const skill of skills) {
            const words = await this.getSkillWords(skill.id)
            words.forEach(word => wordsToSkills.set(word, skill.id))
        }
        const words = Array.from(wordsToSkills.keys())

        const translations = []
        const batchSize = 50
        for (let i = 0; i < words.length; i += batchSize) {
            const batch = words.slice(i, i + batchSize)
            const data = await this.translate(course.id, batch)
            batch.forEach((word, i) => {
                translations.push({
                    from: course.learningLanguage.id,
                    to: course.fromLanguage.id,
                    word,
                    translations: data[i],
                    skills: wordsToSkills.get(word),
                })
            })
        }
        translations.sort((a, b) => a.word.localeCompare(b.word))
        await this.writeWords(translations)
    }

    async getCourses() {
        console.log('getCourses')
        return await cache(`courses.json`, () => this.duolingo.getCourses())
    }

    async getCourseSkills(courseId) {
        console.log('getCourseSkills:', courseId)
        return await cache(`skills/${courseId}.json`,
                           () => this.duolingo.getCourseSkills(courseId))
    }

    async getSkillWords(skillId) {
        console.log('getSkillWords:', skillId)
        return await cache(`words/${skillId}.json`,
                           () => this.duolingo.getSkillWords(skillId))
    }

    async translate(courseId, words) {
        console.log('translate:', courseId, '[' + words + ']')
        const hash = sha1(JSON.stringify(words))
        return await cache(`translate/${courseId}/${hash}.json`,
                           () => this.duolingo.translate(courseId, words))
    }

    async writeLanguages(languages) {
        const col = this.mongo.db('langquiz').collection('languages')
        const ops = languages.map(language => ({
            replaceOne: {
                filter: {
                    // this tuple uniquely identifies document
                    id: language.id,
                },
                replacement: language,
                upsert: true,
            },
        }))
        return this.write(col, ops)
    }

    async writeSkills(skills) {
        const col = this.mongo.db('langquiz').collection('skills')
        const ops = skills.map(skill => ({
            replaceOne: {
                filter: {
                    // this tuple uniquely identifies document
                    id: skill.id,
                },
                replacement: skill,
                upsert: true,
            },
        }))
        return this.write(col, ops)
    }

    async writeWords(words) {
        const col = this.mongo.db('langquiz').collection('words2')
        const ops = words.map(word => ({
            replaceOne: {
                filter: {
                    // this tuple uniquely identifies document
                    from: word.from,
                    to: word.to,
                    word: word.word,
                },
                replacement: word,
                upsert: true,
            },
        }))
        return this.write(col, ops)
    }

    async write(col, ops) {
        const result = await col.bulkWrite(ops)
        console.log(`Collection ${col.collectionName}:`,
                    `${result.upsertedCount} inserted,`,
                    `${result.matchedCount} matched,`,
                    `${result.modifiedCount} updated`)
    }
}

main().catch(console.error)
