using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging;

namespace EqClient.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class CalculationController : ControllerBase
    {

        private readonly ILogger<CalculationController> _logger;

        public CalculationController(ILogger<CalculationController> logger)
        {
            _logger = logger;
        }

        [HttpGet]
        public IActionResult Get()
        {
            return Ok();
        }
    }
}
