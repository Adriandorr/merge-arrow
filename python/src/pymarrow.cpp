#include "marrow/api.h"
#include <arrow/python/pyarrow.h>
#include <pybind11/pybind11.h>
#include <pybind11/stl.h>

void load_pyarrow() {
    static bool loaded_pyarrow = false;
    if (!loaded_pyarrow) {
        if (arrow::py::import_pyarrow() < 0) {
            throw std::runtime_error("Failed to load pyarrow");
        }
        loaded_pyarrow = true;
    }
}

int test(int i) {
    return 1 + i;
}
PYBIND11_MODULE(pymarrow, m) {
    load_pyarrow();
    m.def("test", &test, "testing");
    m.def("add_index", &marrow::api::add_index, "Add an index column and meta data, which can be used by the sort and merge methods.", pybind11::arg("batch"), pybind11::arg("on"));
    m.def("sort", &marrow::api::sort, "Sort the record batch by the specified columns. If an index column is present it uses that.", pybind11::arg("batch"), pybind11::arg("on"));
    m.def("merge", &marrow::api::merge, "Do a left, inner or outer merge. If the table has either an index or is sorted (and has the required meta data as added by the add_index and sort methods), it will use those, otherwise it will create a temporary index",
        pybind11::arg("left"), pybind11::arg("right"), pybind11::arg("on"), pybind11::arg("how"), pybind11::arg("right_postfix") = "");

}
