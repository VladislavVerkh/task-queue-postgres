package dev.verkhovskiy.taskqueue.rest;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.isNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.get;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.jsonPath;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.OperationResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.RequeueDeadLetterRequest;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.RequeueDeadLetterResponse;
import dev.verkhovskiy.taskqueue.rest.dto.TaskQueueAdminDtos.SummaryResponse;
import java.time.Duration;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

@ExtendWith(MockitoExtension.class)
class TaskQueueAdminControllerTest {

  @Mock private TaskQueueAdminService adminService;

  private MockMvc mockMvc;

  @BeforeEach
  void setUp() {
    mockMvc =
        MockMvcBuilders.standaloneSetup(new TaskQueueAdminController(adminService))
            .setControllerAdvice(new TaskQueueRestExceptionHandler())
            .build();
  }

  @Test
  void summaryUsesLibraryRestBasePath() throws Exception {
    when(adminService.summary()).thenReturn(new SummaryResponse(10, 2, 30, 5, 4, 1));

    mockMvc
        .perform(get("/task-queue/admin/v1/summary"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.readyTasks").value(10))
        .andExpect(jsonPath("$.deadLetters").value(4))
        .andExpect(jsonPath("$.drainingPartitions").value(1));
  }

  @Test
  void commandEndpointReturnsJsonResponse() throws Exception {
    when(adminService.rebalance()).thenReturn(new OperationResponse("completed"));

    mockMvc
        .perform(post("/task-queue/admin/v1/rebalance"))
        .andExpect(status().isOk())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_JSON))
        .andExpect(jsonPath("$.status").value("completed"));
  }

  @Test
  void requeueNotFoundReturnsProblemJson() throws Exception {
    UUID taskId = UUID.randomUUID();
    when(adminService.requeueDeadLetter(eq(taskId), isNull()))
        .thenThrow(TaskQueueRestException.notFound("Dead-letter task not found: " + taskId));

    mockMvc
        .perform(post("/task-queue/admin/v1/dead-letters/{taskId}/requeue", taskId))
        .andExpect(status().isNotFound())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
        .andExpect(jsonPath("$.title").value("Not found"))
        .andExpect(jsonPath("$.detail").value("Dead-letter task not found: " + taskId));
  }

  @Test
  void requeueAcceptsDatabaseRelativeDelayBody() throws Exception {
    UUID taskId = UUID.randomUUID();
    when(adminService.requeueDeadLetter(eq(taskId), any(RequeueDeadLetterRequest.class)))
        .thenReturn(new RequeueDeadLetterResponse(taskId, true));

    mockMvc
        .perform(
            post("/task-queue/admin/v1/dead-letters/{taskId}/requeue", taskId)
                .contentType(MediaType.APPLICATION_JSON)
                .content("{\"availableAfter\":\"PT30S\"}"))
        .andExpect(status().isOk())
        .andExpect(jsonPath("$.taskId").value(taskId.toString()))
        .andExpect(jsonPath("$.requeued").value(true));

    ArgumentCaptor<RequeueDeadLetterRequest> requestCaptor =
        ArgumentCaptor.forClass(RequeueDeadLetterRequest.class);
    verify(adminService).requeueDeadLetter(eq(taskId), requestCaptor.capture());
    assertThat(requestCaptor.getValue().availableAfter()).isEqualTo(Duration.ofSeconds(30));
  }

  @Test
  void invalidUuidReturnsProblemJson() throws Exception {
    mockMvc
        .perform(post("/task-queue/admin/v1/dead-letters/not-a-uuid/requeue"))
        .andExpect(status().isBadRequest())
        .andExpect(content().contentTypeCompatibleWith(MediaType.APPLICATION_PROBLEM_JSON))
        .andExpect(jsonPath("$.title").value("Bad request"));
  }
}
