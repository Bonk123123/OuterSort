"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __generator = (this && this.__generator) || function (thisArg, body) {
    var _ = { label: 0, sent: function() { if (t[0] & 1) throw t[1]; return t[1]; }, trys: [], ops: [] }, f, y, t, g;
    return g = { next: verb(0), "throw": verb(1), "return": verb(2) }, typeof Symbol === "function" && (g[Symbol.iterator] = function() { return this; }), g;
    function verb(n) { return function (v) { return step([n, v]); }; }
    function step(op) {
        if (f) throw new TypeError("Generator is already executing.");
        while (g && (g = 0, op[0] && (_ = 0)), _) try {
            if (f = 1, y && (t = op[0] & 2 ? y["return"] : op[0] ? y["throw"] || ((t = y["return"]) && t.call(y), 0) : y.next) && !(t = t.call(y, op[1])).done) return t;
            if (y = 0, t) op = [op[0] & 2, t.value];
            switch (op[0]) {
                case 0: case 1: t = op; break;
                case 4: _.label++; return { value: op[1], done: false };
                case 5: _.label++; y = op[1]; op = [0]; continue;
                case 7: op = _.ops.pop(); _.trys.pop(); continue;
                default:
                    if (!(t = _.trys, t = t.length > 0 && t[t.length - 1]) && (op[0] === 6 || op[0] === 2)) { _ = 0; continue; }
                    if (op[0] === 3 && (!t || (op[1] > t[0] && op[1] < t[3]))) { _.label = op[1]; break; }
                    if (op[0] === 6 && _.label < t[1]) { _.label = t[1]; t = op; break; }
                    if (t && _.label < t[2]) { _.label = t[2]; _.ops.push(op); break; }
                    if (t[2]) _.ops.pop();
                    _.trys.pop(); continue;
            }
            op = body.call(thisArg, _);
        } catch (e) { op = [6, e]; y = 0; } finally { f = t = 0; }
        if (op[0] & 5) throw op[1]; return { value: op[0] ? op[1] : void 0, done: true };
    }
};
var __asyncValues = (this && this.__asyncValues) || function (o) {
    if (!Symbol.asyncIterator) throw new TypeError("Symbol.asyncIterator is not defined.");
    var m = o[Symbol.asyncIterator], i;
    return m ? m.call(o) : (o = typeof __values === "function" ? __values(o) : o[Symbol.iterator](), i = {}, verb("next"), verb("throw"), verb("return"), i[Symbol.asyncIterator] = function () { return this; }, i);
    function verb(n) { i[n] = o[n] && function (v) { return new Promise(function (resolve, reject) { v = o[n](v), settle(resolve, reject, v.done, v.value); }); }; }
    function settle(resolve, reject, d, v) { Promise.resolve(v).then(function(v) { resolve({ value: v, done: d }); }, reject); }
};
Object.defineProperty(exports, "__esModule", { value: true });
var fs = require("fs");
var readline = require("readline");
var util = require("util");
var stream = require("stream");
var lineByLine = require("n-readlines"); // old module
// 1tb static memory
// 500mb dynamic memory
// 1 str < 1_000_000 chars
// function when stream finish
var finished = util.promisify(stream.finished);
// split to files by chunks
function SplitToFiles(filename, chunk_size) {
    var _a, e_1, _b, _c;
    return __awaiter(this, void 0, void 0, function () {
        var writeStreamLeft, writeStreamRight, readStream, rl, flag, counter, segments, _d, rl_1, rl_1_1, line, e_1_1;
        return __generator(this, function (_e) {
            switch (_e.label) {
                case 0:
                    writeStreamLeft = fs.createWriteStream("left.txt");
                    writeStreamRight = fs.createWriteStream("right.txt");
                    readStream = fs.createReadStream(filename);
                    rl = readline.createInterface({
                        input: readStream,
                        crlfDelay: Infinity,
                    });
                    flag = true;
                    counter = 0;
                    segments = 1;
                    _e.label = 1;
                case 1:
                    _e.trys.push([1, 6, 7, 12]);
                    _d = true, rl_1 = __asyncValues(rl);
                    _e.label = 2;
                case 2: return [4 /*yield*/, rl_1.next()];
                case 3:
                    if (!(rl_1_1 = _e.sent(), _a = rl_1_1.done, !_a)) return [3 /*break*/, 5];
                    _c = rl_1_1.value;
                    _d = false;
                    line = _c;
                    if (counter == chunk_size) {
                        flag = !flag;
                        counter = 0;
                        segments++;
                    }
                    if (flag) {
                        writeStreamLeft.write(line + "\n");
                    }
                    else {
                        writeStreamRight.write(line + "\n");
                    }
                    counter++;
                    _e.label = 4;
                case 4:
                    _d = true;
                    return [3 /*break*/, 2];
                case 5: return [3 /*break*/, 12];
                case 6:
                    e_1_1 = _e.sent();
                    e_1 = { error: e_1_1 };
                    return [3 /*break*/, 12];
                case 7:
                    _e.trys.push([7, , 10, 11]);
                    if (!(!_d && !_a && (_b = rl_1.return))) return [3 /*break*/, 9];
                    return [4 /*yield*/, _b.call(rl_1)];
                case 8:
                    _e.sent();
                    _e.label = 9;
                case 9: return [3 /*break*/, 11];
                case 10:
                    if (e_1) throw e_1.error;
                    return [7 /*endfinally*/];
                case 11: return [7 /*endfinally*/];
                case 12:
                    //
                    // wait end of the streams
                    writeStreamLeft.end();
                    writeStreamRight.end();
                    return [4 /*yield*/, finished(writeStreamLeft)];
                case 13:
                    _e.sent();
                    return [4 /*yield*/, finished(writeStreamRight)];
                case 14:
                    _e.sent();
                    //
                    return [2 /*return*/, segments];
            }
        });
    });
}
// take chunk in dynamic memory, sort and write to file
function SplitToFilesWithSort(filename, chunk_size) {
    var _a, e_2, _b, _c;
    return __awaiter(this, void 0, void 0, function () {
        var writeStreamLeft, writeStreamRight, readStream, rl, flag, chunk, segments, _d, rl_2, rl_2_1, line, e_2_1;
        return __generator(this, function (_e) {
            switch (_e.label) {
                case 0:
                    writeStreamLeft = fs.createWriteStream("left.txt");
                    writeStreamRight = fs.createWriteStream("right.txt");
                    readStream = fs.createReadStream(filename);
                    rl = readline.createInterface({
                        input: readStream,
                        crlfDelay: Infinity,
                    });
                    flag = true;
                    chunk = [];
                    segments = 1;
                    _e.label = 1;
                case 1:
                    _e.trys.push([1, 6, 7, 12]);
                    _d = true, rl_2 = __asyncValues(rl);
                    _e.label = 2;
                case 2: return [4 /*yield*/, rl_2.next()];
                case 3:
                    if (!(rl_2_1 = _e.sent(), _a = rl_2_1.done, !_a)) return [3 /*break*/, 5];
                    _c = rl_2_1.value;
                    _d = false;
                    line = _c;
                    chunk.push(line);
                    if (chunk.length >= chunk_size) {
                        chunk.sort();
                        if (flag) {
                            writeStreamLeft.write(chunk.join("\n") + "\n");
                        }
                        else {
                            writeStreamRight.write(chunk.join("\n") + "\n");
                        }
                        flag = !flag;
                        chunk = [];
                        segments++;
                    }
                    _e.label = 4;
                case 4:
                    _d = true;
                    return [3 /*break*/, 2];
                case 5: return [3 /*break*/, 12];
                case 6:
                    e_2_1 = _e.sent();
                    e_2 = { error: e_2_1 };
                    return [3 /*break*/, 12];
                case 7:
                    _e.trys.push([7, , 10, 11]);
                    if (!(!_d && !_a && (_b = rl_2.return))) return [3 /*break*/, 9];
                    return [4 /*yield*/, _b.call(rl_2)];
                case 8:
                    _e.sent();
                    _e.label = 9;
                case 9: return [3 /*break*/, 11];
                case 10:
                    if (e_2) throw e_2.error;
                    return [7 /*endfinally*/];
                case 11: return [7 /*endfinally*/];
                case 12:
                    //
                    // if there are unfinished lines
                    if (chunk.length > 0) {
                        chunk.sort();
                        if (flag) {
                            writeStreamLeft.write(chunk.join("\n") + "\n");
                        }
                        else {
                            writeStreamRight.write(chunk.join("\n") + "\n");
                        }
                    }
                    //
                    // wait end of the streams
                    writeStreamLeft.end();
                    writeStreamRight.end();
                    return [4 /*yield*/, finished(writeStreamLeft)];
                case 13:
                    _e.sent();
                    return [4 /*yield*/, finished(writeStreamRight)];
                case 14:
                    _e.sent();
                    //
                    return [2 /*return*/, segments];
            }
        });
    });
}
// merge files
function MergeFiles(filename, chunk_size) {
    return __awaiter(this, void 0, void 0, function () {
        var writeStreamFile, readfileStreamLeft, readfileStreamRight, leftElement, rightElement, leftCounter, rightCounter, currentRecord, flag, leftLineString, rightLineString;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0:
                    writeStreamFile = fs.createWriteStream(filename);
                    readfileStreamLeft = new lineByLine("left.txt");
                    readfileStreamRight = new lineByLine("right.txt");
                    leftElement = readfileStreamLeft.next();
                    rightElement = readfileStreamRight.next();
                    leftCounter = 0;
                    rightCounter = 0;
                    //
                    // merge
                    while (leftElement || rightElement) {
                        currentRecord = void 0;
                        flag = false;
                        leftLineString = leftElement.toString() + "\n";
                        rightLineString = rightElement.toString() + "\n";
                        // if no element left (or element right) or chunk ended. else check both lines
                        if (!leftElement || leftCounter == chunk_size) {
                            currentRecord = rightLineString;
                        }
                        else if (!rightElement || rightCounter == chunk_size) {
                            currentRecord = leftLineString;
                            flag = true;
                        }
                        else {
                            // A < a (check on that)
                            if (leftLineString.toLowerCase() < rightLineString.toLowerCase()) {
                                currentRecord = leftLineString;
                                flag = true;
                            }
                            else {
                                currentRecord = rightLineString;
                            }
                        }
                        // write the reslting string
                        writeStreamFile.write(currentRecord);
                        // next line
                        if (flag) {
                            leftElement = readfileStreamLeft.next();
                            leftCounter++;
                        }
                        else {
                            rightElement = readfileStreamRight.next();
                            rightCounter++;
                        }
                        // if both chunk ended counters = 0
                        if (leftCounter != chunk_size || rightCounter != chunk_size) {
                            continue;
                        }
                        leftCounter = 0;
                        rightCounter = 0;
                    }
                    //
                    // wait end of the stream
                    writeStreamFile.end();
                    return [4 /*yield*/, finished(writeStreamFile)];
                case 1:
                    _a.sent();
                    return [2 /*return*/];
            }
        });
    });
}
function OuterSort(filename, chunk_size) {
    return __awaiter(this, void 0, void 0, function () {
        var segments, segments_1;
        return __generator(this, function (_a) {
            switch (_a.label) {
                case 0: return [4 /*yield*/, SplitToFilesWithSort(filename, chunk_size)];
                case 1:
                    segments = _a.sent();
                    return [4 /*yield*/, MergeFiles(filename, chunk_size)];
                case 2:
                    _a.sent();
                    chunk_size *= 2;
                    _a.label = 3;
                case 3:
                    if (!true) return [3 /*break*/, 6];
                    return [4 /*yield*/, SplitToFiles(filename, chunk_size)];
                case 4:
                    segments_1 = _a.sent();
                    if (segments_1 == 1)
                        return [3 /*break*/, 6];
                    return [4 /*yield*/, MergeFiles(filename, chunk_size)];
                case 5:
                    _a.sent();
                    chunk_size *= 2;
                    return [3 /*break*/, 3];
                case 6:
                    //
                    // delete help files
                    fs.unlink("left.txt", function () { });
                    fs.unlink("right.txt", function () { });
                    return [2 /*return*/];
            }
        });
    });
}
// test
(function () { return __awaiter(void 0, void 0, void 0, function () {
    function generateRandomString(length) {
        var result = "";
        var characters = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz";
        for (var i = 0; i < length; i++) {
            result += characters.charAt(Math.floor(Math.random() * characters.length));
        }
        return result;
    }
    var testStream, linesQuntity, charsQuntity, i, lengthOfString;
    return __generator(this, function (_a) {
        switch (_a.label) {
            case 0:
                testStream = fs.createWriteStream("input.txt");
                linesQuntity = 5000;
                charsQuntity = 1000;
                for (i = 0; i < linesQuntity; i++) {
                    lengthOfString = Math.random() * charsQuntity + 1;
                    testStream.write(generateRandomString(lengthOfString) + "\n");
                }
                testStream.end();
                return [4 /*yield*/, finished(testStream)];
            case 1:
                _a.sent();
                // if we have 500 Mb dm
                // we can take approximately 400 lines
                OuterSort("input.txt", 400);
                return [2 /*return*/];
        }
    });
}); })();
