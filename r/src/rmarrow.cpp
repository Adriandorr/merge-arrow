#include "marrow/api.h"
#include <Rcpp.h>

std::shared_ptr<arrow::RecordBatch> from_r(SEXP batch_sexp) {
	Rcpp::traits::input_parameter<const std::shared_ptr<arrow::RecordBatch>&>::type batch(batch_sexp);
	Rcpp::traits::input_parameter<int>::type i(i_sexp);
    return batch;
}

// [[Rcpp::export]]
SEXP r_add_index(SEXP batch, std::vector<std::string> on) {
    return Rcpp::wrap(marrow::api::add_index(from_r(batch), on));
}

// [[Rcpp::export]]
SEXP r_sort(SEXP batch, std::vector<std::string> on) {
    return SEXP(marrow::api::sort(from_r(batch), on));
}

// [[Rcpp::export]]
SEXP merge(SEXP left, SEXP right, std::vector<std::string> on, std::string how = "inner", std::string right_postfix = "") {
    return Rcpp::wrap(marrow::api::merge(from_r(left), from_r(right), on, how, right_postfix));
}
