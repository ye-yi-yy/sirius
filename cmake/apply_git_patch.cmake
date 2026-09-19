execute_process(
  COMMAND "${GIT_EXECUTABLE}" apply --check "${PATCH_FILE}"
  WORKING_DIRECTORY "${SOURCE_DIR}"
  RESULT_VARIABLE forward_result
  ERROR_VARIABLE forward_error)

if(forward_result EQUAL 0)
  execute_process(
    COMMAND "${GIT_EXECUTABLE}" apply --whitespace=nowarn "${PATCH_FILE}"
    WORKING_DIRECTORY "${SOURCE_DIR}" COMMAND_ERROR_IS_FATAL ANY)
  return()
endif()

execute_process(
  COMMAND "${GIT_EXECUTABLE}" apply --reverse --check "${PATCH_FILE}"
  WORKING_DIRECTORY "${SOURCE_DIR}"
  RESULT_VARIABLE reverse_result
  ERROR_QUIET)

if(NOT reverse_result EQUAL 0)
  message(FATAL_ERROR "Cannot apply ${PATCH_FILE}: ${forward_error}")
endif()
