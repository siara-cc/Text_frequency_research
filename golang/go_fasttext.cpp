#include <stdio.h>     // fprintf
#include <stdlib.h>    // free
#include <fasttext/fasttext.h>
#include <string.h>
#include <string>
#include <sstream>

using namespace std;

fasttext::FastText ftext;

void printPredictions(
    const vector<pair<fasttext::real, string>>& predictions,
    bool printProb,
    bool multiline) {
  bool first = true;
  for (const auto& prediction : predictions) {
    if (!first && !multiline) {
      cout << " ";
    }
    first = false;
    cout << prediction.second;
    if (printProb) {
      cout << " " << prediction.first;
    }
    if (multiline) {
      cout << endl;
    }
  }
  if (!multiline) {
    cout << endl;
  }
}

extern "C" {

void load_model(const char *filename) {
    ftext.loadModel(string(filename));
}

void predict(const char *input, char *out) {
    int32_t k = 1;
    fasttext::real threshold = 0.0;
    stringstream ioss((std::string(input)));
    vector<pair<fasttext::real, string>> predictions;
    ftext.predictLine(ioss, predictions, k, threshold);
    for (const auto& prediction : predictions) {
      strcpy(out, prediction.second.c_str());
      break;
    }
}

}

